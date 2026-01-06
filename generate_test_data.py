import json
import requests
import time
from datetime import datetime, timedelta
from faker import Faker
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import argparse
import random

fake = Faker('vi_VN')

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CONFIGURATION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class Config:
    API_URL = "http://localhost:8080/v1/profiles/track"
    API_KEY = "cdp_demo_demo"  # ✅ Default API Key
    NUM_EVENTS = 1000
    NUM_UNIQUE_IDCARDS = 700
    BATCH_SIZE = 50
    DELAY_BETWEEN_BATCHES = 0.5
    SAVE_TO_FILE = True
    SHOW_PROGRESS = True

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DATA POOLS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

TYPES = ['login', 'register', 'update_profile', 'purchase', 'view_product']
GENDERS = ['male', 'female', 'other']
RELIGIONS = ['Buddhism', 'Catholic', 'None', 'Protestant', 'Other']
OS_LIST = ['Windows 10', 'Windows 11', 'macOS', 'Ubuntu', 'iOS', 'Android']
DEVICES = ['Desktop', 'MacBook Pro', 'iPhone 15', 'Samsung Galaxy S24', 'iPad', 'Laptop']
BROWSERS = ['Chrome', 'Safari', 'Firefox', 'Edge', 'Opera']
UTM_SOURCES = ['google', 'facebook', 'instagram', 'direct', 'email', 'tiktok']
UTM_CAMPAIGNS = ['summer_sale', 'winter_promo', 'none', 'black_friday', 'new_year']
UTM_MEDIUMS = ['cpc', 'organic', 'email', 'social', 'referral']
REG_SOURCES = ['web_desktop', 'web_mobile', 'ios_app', 'android_app', 'api']

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# EVENT GENERATOR
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def generate_event(index, idcard_pool, old_idcard_pool):
    """Generate a single profile event"""
    
    # Random idcard (có khả năng trùng để merge)
    idcard_index = random.randint(0, len(idcard_pool) - 1)
    idcard = idcard_pool[idcard_index]
    old_idcard = old_idcard_pool[idcard_index]
    
    # Generate random datetime trong 30 ngày qua
    days_ago = random.randint(0, 30)
    hours_ago = random.randint(0, 23)
    minutes_ago = random.randint(0, 59)
    event_time = datetime.now() - timedelta(days=days_ago, hours=hours_ago, minutes=minutes_ago)
    first_seen = event_time - timedelta(days=random.randint(0, 365))
    
    # Random user
    first_name = fake.first_name()
    last_name = fake.last_name()
    full_name = f"{last_name} {first_name}"
    
    event = {
        "type": random.choice(TYPES),
        "user_id": f"user_{random.choice(['desktop', 'mobile', 'tablet'])}_{str(index).zfill(5)}",
        "traits": {
            "full_name": full_name,
            "first_name": first_name,
            "last_name": last_name,
            "idcard": idcard,
            "old_idcard": old_idcard,
            "phone": fake.phone_number(),
            "email": fake.email(),
            "gender": random.choice(GENDERS),
            "dob": fake.date_of_birth(minimum_age=18, maximum_age=80).strftime("%Y-%m-%d"),
            "address": fake.address(),
            "religion": random.choice(RELIGIONS)
        },
        "platforms": {
            "os": random.choice(OS_LIST),
            "device": random.choice(DEVICES),
            "browser": random.choice(BROWSERS),
            "app_version": f"{random.randint(1, 3)}.{random.randint(0, 9)}.{random.randint(0, 9)}"
        },
        "campaign": {
            "utm_source": random.choice(UTM_SOURCES),
            "utm_campaign": random.choice(UTM_CAMPAIGNS),
            "utm_medium": random.choice(UTM_MEDIUMS)
        },
        "metadata": {
            "first_seen_at": first_seen.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "last_seen_at": event_time.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "registration_source": random.choice(REG_SOURCES),
            "user_agent": fake.user_agent(),
            "login_count": random.randint(1, 100)
        }
    }
    
    return event

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# API SENDER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def send_event(event, index, config):
    """Send a single event to API"""
    try:
        headers = {
            "Content-Type": "application/json",
            "X-API-Key": config.API_KEY  # ✅ Add API Key to headers
        }
        
        response = requests.post(
            config.API_URL,
            json=event,
            headers=headers,
            timeout=10
        )
        
        return {
            "index": index,
            "status_code": response.status_code,
            "success": response.status_code in [200, 201],
            "idcard": event['traits']['idcard'],
            "type": event['type'],
            "response": response.text[:200] if response.status_code >= 400 else None
        }
    except Exception as e:
        return {
            "index": index,
            "status_code": 0,
            "success": False,
            "idcard": event['traits']['idcard'],
            "type": event['type'],
            "error": str(e)
        }

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# BATCH PROCESSING
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def send_events_batch(events, config, max_workers=10):
    """Send events in batches with threading"""
    results = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        
        for i, event in enumerate(events, 1):
            future = executor.submit(send_event, event, i, config)
            futures.append(future)
        
        if config.SHOW_PROGRESS:
            with tqdm(total=len(events), desc="📤 Sending events") as pbar:
                for future in as_completed(futures):
                    result = future.result()
                    results.append(result)
                    pbar.update(1)
        else:
            for future in as_completed(futures):
                result = future.result()
                results.append(result)
    
    return results

def send_events_sequential(events, config):
    """Send events sequentially with progress bar"""
    results = []
    
    if config.SHOW_PROGRESS:
        iterator = tqdm(enumerate(events, 1), total=len(events), desc="📤 Sending events")
    else:
        iterator = enumerate(events, 1)
    
    for i, event in iterator:
        result = send_event(event, i, config)
        results.append(result)
        time.sleep(0.01)  # Small delay to avoid overwhelming server
    
    return results

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# STATISTICS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def print_statistics(results, events):
    """Print detailed statistics"""
    success_count = sum(1 for r in results if r['success'])
    failed_count = len(results) - success_count
    
    # Status code distribution
    status_codes = {}
    for r in results:
        code = r['status_code']
        status_codes[code] = status_codes.get(code, 0) + 1
    
    # Type distribution
    type_dist = {}
    for e in events:
        t = e['type']
        type_dist[t] = type_dist.get(t, 0) + 1
    
    # IDCard duplicates
    idcard_count = {}
    for e in events:
        idcard = e['traits']['idcard']
        idcard_count[idcard] = idcard_count.get(idcard, 0) + 1
    
    duplicate_idcards = {k: v for k, v in idcard_count.items() if v > 1}
    
    print("\n" + "="*60)
    print("📊 STATISTICS REPORT")
    print("="*60)
    
    print(f"\n✅ SUCCESS: {success_count}/{len(results)} ({success_count/len(results)*100:.1f}%)")
    print(f"❌ FAILED:  {failed_count}/{len(results)} ({failed_count/len(results)*100:.1f}%)")
    
    print(f"\n📈 Status Codes:")
    for code, count in sorted(status_codes.items()):
        print(f"   {code}: {count} requests")
    
    print(f"\n📋 Event Types:")
    for event_type, count in sorted(type_dist.items(), key=lambda x: x[1], reverse=True):
        print(f"   {event_type}: {count} events")
    
    print(f"\n🔄 Merge Information:")
    print(f"   Unique IDCards: {len(idcard_count)}")
    print(f"   IDCards with duplicates: {len(duplicate_idcards)}")
    print(f"   Total events that will merge: {sum(duplicate_idcards.values())}")
    
    if duplicate_idcards:
        print(f"\n🔝 Top 10 IDCards with most events:")
        sorted_duplicates = sorted(duplicate_idcards.items(), key=lambda x: x[1], reverse=True)[:10]
        for idcard, count in sorted_duplicates:
            print(f"   {idcard}: {count} events")
    
    # Show failed requests
    if failed_count > 0:
        print(f"\n❌ Failed Requests (showing first 10):")
        failed = [r for r in results if not r['success']][:10]
        for r in failed:
            error_msg = r.get('error') or r.get('response', 'Unknown error')
            print(f"   #{r['index']} - IDCard: {r['idcard'][:10]}... - Error: {error_msg}")
    
    print("\n" + "="*60)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# MAIN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def main():
    parser = argparse.ArgumentParser(description='Send profile events to CDP API')
    parser.add_argument('-n', '--num-events', type=int, default=Config.NUM_EVENTS, 
                        help='Number of events to generate')
    parser.add_argument('-u', '--url', type=str, default=Config.API_URL, 
                        help='API endpoint URL')
    parser.add_argument('-k', '--api-key', type=str, default=Config.API_KEY,
                        help='API Key for authentication')  # ✅ Add API Key argument
    parser.add_argument('-m', '--mode', choices=['batch', 'sequential'], default='batch', 
                        help='Sending mode')
    parser.add_argument('-w', '--workers', type=int, default=10, 
                        help='Number of concurrent workers (batch mode)')
    parser.add_argument('--no-save', action='store_true', 
                        help='Do not save events to file')
    parser.add_argument('--no-progress', action='store_true', 
                        help='Hide progress bar')
    parser.add_argument('--unique-idcards', type=int, default=Config.NUM_UNIQUE_IDCARDS,
                        help='Number of unique IDCards (less = more merges)')
    
    args = parser.parse_args()
    
    # Create config from args
    config = Config()
    config.API_URL = args.url
    config.API_KEY = args.api_key  # ✅ Set API Key from args
    config.NUM_EVENTS = args.num_events
    config.SAVE_TO_FILE = not args.no_save
    config.SHOW_PROGRESS = not args.no_progress
    config.NUM_UNIQUE_IDCARDS = args.unique_idcards
    
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print("🚀 CDP PROFILE EVENT SENDER")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print(f"📍 API URL: {config.API_URL}")
    print(f"🔑 API Key: {config.API_KEY}")  # ✅ Show API Key
    print(f"📊 Events to send: {config.NUM_EVENTS}")
    print(f"🆔 Unique IDCards: {config.NUM_UNIQUE_IDCARDS}")
    print(f"🔄 Mode: {args.mode}")
    if args.mode == 'batch':
        print(f"👥 Workers: {args.workers}")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    
    # Generate ID pools
    print(f"\n Generating ID pools...")
    idcard_pool = [f"{fake.random_int(min=100000000000, max=100000000095)}" 
                   for _ in range(config.NUM_UNIQUE_IDCARDS)]
    old_idcard_pool = [f"{fake.random_int(min=100000000, max=999999999)}" 
                       for _ in range(config.NUM_UNIQUE_IDCARDS)]
    
    # Generate events
    print(f"🏗️  Generating {config.NUM_EVENTS} events...")
    events = [generate_event(i, idcard_pool, old_idcard_pool) 
              for i in range(1, config.NUM_EVENTS + 1)]
    print(f"✅ Generated {len(events)} events")
    
    # Save to file
    if config.SAVE_TO_FILE:
        filename = f"generated_events_{config.NUM_EVENTS}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(events, f, ensure_ascii=False, indent=2)
        print(f"💾 Saved to: {filename}")
    
    # Send events
    start_time = time.time()
    
    if args.mode == 'batch':
        results = send_events_batch(events, config, max_workers=args.workers)
    else:
        results = send_events_sequential(events, config)
    
    elapsed_time = time.time() - start_time
    
    # Print statistics
    print_statistics(results, events)
    
    print(f"\n⏱️  Total time: {elapsed_time:.2f}s")
    print(f"📈 Throughput: {len(events)/elapsed_time:.2f} events/sec")
    
    # Save results
    if config.SAVE_TO_FILE:
        results_filename = f"results_{config.NUM_EVENTS}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(results_filename, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        print(f"💾 Results saved to: {results_filename}")
    
    print("\n✅ Done!")

if __name__ == "__main__":
    main()