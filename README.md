# CDP Platform

> **Customer Data Platform (CDP)** – Hệ thống thu thập, xử lý, quản lý dữ liệu khách hàng đa kênh, phục vụ phân khúc, hành trình, chiến dịch và phân tích hành vi.

Monorepo này bao gồm toàn bộ **microservices**, **tài liệu**, **hạ tầng**, **SDK**, **frontend**, **CI/CD**, **tooling** và **runbook** cho hệ thống CDP.

---

## 📌 Mục tiêu của dự án

- Xây dựng một **CDP hiện đại**, đa tenant, có khả năng scale lớn.
- Chuẩn hóa luồng **ingestion → processing → enrichment → indexing → segmentation → activation**.
- Cung cấp bộ API chuẩn hóa cho:
  - **Tracking**
  - **Profiles / Devices**
  - **Segments**
  - **Journeys**
  - **Campaigns**
  - **Admin portal**
- Đảm bảo **tính toàn vẹn dữ liệu**, **hiệu suất cao**, **độ trễ thấp**, **khả năng mở rộng**.
- Định nghĩa đầy đủ tài liệu: kiến trúc, domain, sequence diagram, API, ops, ADR.

---

## 🏗️ Kiến trúc tổng quan

Sơ đồ kiến trúc tổng quan (chi tiết xem tại [ARCHITECTURE.md](docs/overview/ARCHITECTURE.md)):

## 🏗️ Mô tả về cấu trúc code, coding standard
Chi tiết xem tại [coding-standards.md](docs/overview/coding-standards.md):
