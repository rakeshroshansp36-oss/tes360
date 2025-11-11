import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
import os
from datetime import datetime

start_id = 21221773
end_id = 21223052
# Generate all URLs from start_id to end_id (inclusive!)
urls = [f"https://zoma.to/r/{i}" for i in range(start_id, end_id+1)]

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
}

def check_url_accurate(url):
    """
    Check URL with EXACT same logic as original script.
    Uses individual requests (not session) to match original behavior exactly.
    """
    try:
        # EXACT same request as original script - no session, individual requests
        response = requests.get(url, headers=headers, allow_redirects=True, timeout=8)
        # EXACT same status logic as original script
        status = "Working" if response.status_code == 200 else f"Not Working ({response.status_code})"
    except Exception as e:
        # EXACT same error handling as original script
        status = "Not Working (Error)"
    return {"URL": url, "Status": status}

# Setup real-time saving files
output_csv = "zomato_url_status_fast_Demo1_realtime.csv"
output_excel = "zomato_url_status_fast_Demo1_realtime.xlsx"

# Create CSV file with headers immediately
with open(output_csv, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=["URL", "Status"])
    writer.writeheader()
    print(f"📝 Created CSV file: {output_csv}")
    print(f"📊 Excel file will be: {output_excel}")

print(f"\nChecking {len(urls)} URLs with parallel execution...")
print(f"💾 Real-time saving: CSV (immediate) + Excel (every 50 URLs)")
print(f"🔄 Auto-save on error: Data preserved even if script crashes")
print("="*60)

results = []
completed_count = 0
working_count = 0
not_working_count = 0
SAVE_INTERVAL = 50  # Save to Excel every 50 URLs

def save_to_excel(data, filename):
    """Save data to Excel with error handling"""
    try:
        if data:
            df = pd.DataFrame(data)
            df.to_excel(filename, index=False, engine='openpyxl')
            return True
    except Exception as e:
        print(f"⚠️ Excel save warning: {e}")
    return False

try:
    # Use ThreadPoolExecutor with as_completed for real-time processing
    # Maintains 3 workers for accuracy while adding real-time saves
    with ThreadPoolExecutor(max_workers=3) as executor:
        # Submit all tasks and get futures
        future_to_url = {executor.submit(check_url_accurate, url): url for url in urls}
        
        # Process results as they complete (real-time)
        for future in as_completed(future_to_url):
            try:
                result = future.result()
                results.append(result)
                
                # Save to CSV immediately (REAL-TIME SAVE)
                with open(output_csv, 'a', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=["URL", "Status"])
                    writer.writerow(result)
                
                # Update counters
                completed_count += 1
                if result["Status"] == "Working":
                    working_count += 1
                else:
                    not_working_count += 1
                
                # Save to Excel periodically (every SAVE_INTERVAL URLs)
                if completed_count % SAVE_INTERVAL == 0:
                    if save_to_excel(results, output_excel):
                        print(f"💾 Excel updated: {completed_count}/{len(urls)} URLs saved")
                
                # Show progress every 25 URLs
                if completed_count % 25 == 0 or completed_count == len(urls):
                    print(f"✅ Progress: {completed_count}/{len(urls)} URLs checked (CSV saved)")
                    
            except Exception as e:
                print(f"⚠️ Error processing result: {e}")
                
except KeyboardInterrupt:
    print("\n⚠️ Script interrupted by user. Saving data...")
    if results:
        save_to_excel(results, output_excel)
        print(f"💾 Emergency save completed: {len(results)} URLs saved to Excel")
except Exception as e:
    print(f"\n⚠️ Error during execution: {e}. Saving data...")
    if results:
        save_to_excel(results, output_excel)
        print(f"💾 Emergency save completed: {len(results)} URLs saved to Excel")

print("="*60)

# Final save to Excel
if results:
    print("\n💾 Creating final Excel file...")
    save_to_excel(results, output_excel)
    
    # Create DataFrame - EXACT same format as original
    sheet = pd.DataFrame(results)
    print("\n📊 Final Results Summary:")
    print(sheet.head(10))  # Show first 10 rows
    if len(results) > 10:
        print(f"... and {len(results) - 10} more rows")
    print("="*60)
    
    print(f"\n✅ Total URLs checked: {len(results)}")
    print(f"✅ Working: {working_count}")
    print(f"❌ Not Working: {not_working_count}")
    print(f"📊 Success Rate: {working_count}/{len(results)} ({working_count*100//len(results) if len(results) > 0 else 0}%)")
    
    print(f"\n💾 Files saved:")
    print(f"   📄 CSV (Real-time): {os.path.abspath(output_csv)}")
    print(f"   📊 Excel (Final): {os.path.abspath(output_excel)}")
    print(f"\n✅ Real-time save completed successfully!")
    print(f"🔒 Data is safe even if errors occur during execution")
else:
    print("\n⚠️ No results collected. Check for errors above.")
