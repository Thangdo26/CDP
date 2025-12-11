# CDP Platform – Coding Standards & Project Architecture (DDD Guideline)

Version: 1.0
Purpose: Chuẩn hóa khung code cho toàn bộ team CDP Platform.

# 1. Triết lý kiến trúc

**Dự án CDP Platform được thiết kế theo các nguyên tắc:**

- Domain-Driven Design (DDD)
- Hexagonal / Clean Architecture

**Separation of concerns**

- Tách biệt domain logic với infrastructure

- Không để business rule phụ thuộc framework (Spring)

- Dễ mở rộng (scalable), dễ test, dễ bảo trì

# 2. Cấu trúc module tổng thể

**Dự án bao gồm nhiều module nhỏ, mỗi module đại diện một bounded context:**
<pre style="border:1px solid #ccc; padding:12px; border-radius:8px;">
cdp-platform/
├── cdp-app/      # điểm boot chính của project, từ đây sẽ load ứng dụng
├── cdp-auth/     # authen, filters, interceptors
├── cdp-common/   # chứa các thành phần dùng chung cho nhiều module
├── cdp-infra/    # chứa hạ tầng xử lý tương tác với data infra như kafka, redis, mysql, es
├── cdp-ingestion/ # các api events
├── cdp-inbound/  # xử lý event đến
├── cdp-profile/  # module xử lý logic về profile
├── cdp-enrich/ # module tiền xử lý tính toán các metrics để đảm bảo hiệu năng cao cho module segmentation
├── cdp-segmentation/ # module xử lý về segment một tập đối tượng theo điều kiện lọc
├── cdp-campaign/ # xử lý nghiệp vụ liên quan đến chiến dịch marketing
├── cdp-tracking/ # module tracking hiệu quả chiến dịch
└── README.md
</pre>
# 3. Cấu trúc code chuẩn của mỗi module theo DDD

**Mỗi module sẽ có cấu trúc giống nhau:**
<pre style="border:1px solid #ccc; padding:12px; border-radius:8px;">

/src/main/java/com/vft/cdp/<module>/
|
├── application/              # Application services (use cases)
│     ├── dto/                # DTO Request/Response
│     ├── command/            # Command objects
│     ├── query/              # Query objects
│     └── <UseCase>Service.java
|
├── domain/                   # Domain model (pure business logic)
│     ├── model/              # Entities, Value Objects
│     ├── repository/         # Repository interfaces
│     ├── event/              # Domain events
│     └── service/            # Domain services
|
├── infra/                    # Infrastructure implementation
│     ├── persistence/        # JPA/ES repositories implementation
│     ├── es/                 # Elasticsearch documents + adapters
│     ├── kafka/              # Kafka producers / consumers
│     ├── mapper/             # Mapping infra <-> domain
│     └── config/             # Module-specific configs
|
└── api/                      # REST Controller
      ├── request/
      ├── response/
      └── <Module>Controller.java
</pre>      

# 4. Vai trò từng layer
## 4.1. Domain Layer (Trái tim của hệ thống)

**Không phụ thuộc Spring / Database / Framework**

**Bao gồm:**

- Entities (Profile, Segment, Campaign, …)

- Value Objects (Email, UserId, TenantId, …)

- Domain Services (logic phức tạp không nằm trong entity)

- Domain Events

Ví dụ một entity:
<pre style="border:1px solid #ccc; padding:12px; border-radius:8px;">

public class Profile {
    private final TenantId tenantId;
    private final ProfileId profileId;
    private Map<String, Object> traits;

    public void updateTraits(Map<String, Object> updates) {
        traits.putAll(updates);
    }
}

</pre>

## 4.2. Application Layer (Use Case)

***Chỉ orchestrate logic, không chứa business logic nặng.***

- Dùng DTO

- Gọi domain service + repository

- Xử lý transaction

- Publish event

***Ví dụ:***
<pre style="border:1px solid #ccc; padding:12px; border-radius:8px;">
@Service
@RequiredArgsConstructor
public class ProfileService {

    private final ProfileRepository profileRepo;

    public ProfileResponse updateTraits(UpdateTraitsCommand cmd) {
        Profile profile = profileRepo.find(cmd.tenantId(), cmd.profileId())
            .orElseThrow(ProfileNotFound::new);

        profile.updateTraits(cmd.traits());
        profileRepo.save(profile);

        return ProfileMapper.toResponse(profile);
    }
}
</pre>

## 4.3. Infrastructure Layer

**Nhiệm vụ chính:**

- Hiện thực Repository interface trong domain

- Mapping giữa domain model ↔ database model

- Giao tiếp với external systems (ES, Kafka, Redis,...)

**Ví dụ ES Repository:**
<pre style="border:1px solid #ccc; padding:12px; border-radius:8px;">
@Repository
@RequiredArgsConstructor
public class EsProfileRepository implements ProfileRepository {

    private final SpringDataProfileEsRepository springDataRepo;

    @Override
    public Optional<Profile> find(String tenantId, String profileId) {
        return springDataRepo.findByTenantIdAndProfileId(tenantId, profileId)
            .map(ProfileMapper::toDomain);
    }

    @Override
    public void save(Profile profile) {
        springDataRepo.save(ProfileMapper.toDocument(profile));
    }
}

</pre>
## 4.4. API Layer

**Không xử lý business logic.**

**Vai trò:**

- Validate request

- Gọi Application Service

- Mapping DTO ↔ Response JSON

**Ví dụ:**
<pre style="border:1px solid #ccc; padding:12px; border-radius:8px;">
@RestController
@RequiredArgsConstructor
@RequestMapping("/profiles")
public class ProfileController {

    private final ProfileService service;

    @PutMapping("/{tenantId}/{profileId}/traits")
    public ApiResponse updateTraits(
            @PathVariable String tenantId,
            @PathVariable String profileId,
            @RequestBody UpdateTraitsRequest req) {

        return ApiResponse.success(
            service.updateTraits(req.toCommand(tenantId, profileId))
        );
    }
}

</pre>
# 5. Quy tắc coding bắt buộc cho team
## 5.1. Không dùng Entity của DB/ES trong domain

- Domain không biết ElasticsearchDocument, JPAEntity, KafkaModel.

- Domain thuần Java, không annotation Spring.

## 5.2. Repository trong domain chỉ là interface
<pre style="border:1px solid #ccc; padding:12px; border-radius:8px;">
public interface ProfileRepository {
    Optional<Profile> find(String tenantId, String profileId);
    void save(Profile profile);
}
</pre>

- Implementation của nó nằm ở infra/es.

## 5.3. Không viết logic business trong Controller hoặc Infra

<pre style="border:1px solid #ccc; padding:12px; border-radius:8px;">
❌ Sai:

profileDocument.setTraits(req.getTraits());
springDataRepo.save(profileDocument);


✔ Đúng:

Controller → Application Service → Domain → Repo

</pre>

## 5.4. Mọi mapping Domain ↔ ES Document phải qua Mapper
<pre style="border:1px solid #ccc; padding:12px; border-radius:8px;">
public class ProfileMapper {
    public static ProfileDocument toDocument(Profile p) {...}
    public static Profile toDomain(ProfileDocument d) {...}
}
</pre>

## 5.5. Không dùng Map<String, Object> trong domain trừ Value Objects

Domain phải mạnh type, sạch sẽ.

# 6. Ví dụ đầy đủ cho module cdp-profile
<pre style="border:1px solid #ccc; padding:12px; border-radius:8px;">
Domain
cdp-profile/
└── domain/
       model/
       service/
       repository/
       event/

Infrastructure (Elasticsearch)
cdp-profile/
└── infra/es/
       ProfileDocument.java
       SpringDataProfileEsRepository.java
       EsProfileRepository.java   ← adapter implements ProfileRepository
       ProfileMapper.java

Application (Use Cases)
application/
    dto/
    command/
    query/
    ProfileService.java

API
api/
    request/
    response/
    ProfileController.java

</pre>


# 7. Code Flow chuẩn cho mọi nghiệp vụ trong CDP
**Ví dụ flow "Update Profile Traits"**
<pre style="border:1px solid #ccc; padding:12px; border-radius:8px;">
HTTP Request --> Controller
    → Validate request
    → Create Command object
    → Call ProfileService.updateTraits()

Application Layer (ProfileService)
    → Load Domain Profile từ ProfileRepository
    → Gọi domain logic profile.updateTraits()
    → Save Profile back to repository
    → Return DTO

Infra Layer
    → Map Profile → ProfileDocument
    → Save vào Elasticsearch

</pre>

# 8. Testing Strategy
- Unit Test
- Test API using postman
- Integration Test
- E2E Test (Cần khi test segmentation/campaign pipeline)

# 9. Checklist cho lập trình viên trước khi merge code
- Không viết logic business trong Controller
- Domain không phụ thuộc Spring
- Repository domain = interface
- Mọi chuyển đổi DB/ES ↔ Domain dùng Mapper
- Không return document của ES ra API
- API trả DTO, không return Entity
- Unit test cho domain đã đủ
Tên package đúng chuẩn (application/domain/infra/api)	

# 10. Kết luận

**Tài liệu này chuẩn hóa:**

- Organization project structure

- Coding standard theo DDD

- Luồng triển khai feature

- Quy tắc clean code

- Mapping giữa domain & infra

- Hướng dẫn cụ thể để team phát triển đồng nhất

- Tất cả lập trình viên CDP Platform phải tuân thủ tài liệu này khi commit code.
