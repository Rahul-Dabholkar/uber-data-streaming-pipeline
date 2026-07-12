import os
import json
from dotenv import load_dotenv
load_dotenv()

# Add parent path or import from scripts
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from data import generate_uber_ride_confirmation
from connection import send_to_event_hub

def send_forced_ride():
    print("Generating ride confirmation...")
    ride = generate_uber_ride_confirmation()
    
    # Explicitly force the pickup city to Super New York (City ID = 1)
    ride['pickup_city_id'] = 1
    
    print("\nForced Ride Event Details:")
    print(json.dumps(ride, indent=2))
    
    print("\nSending forced ride event to Event Hub...")
    result = send_to_event_hub(ride)
    print(f"Result: {result}")

if __name__ == "__main__":
    send_forced_ride()
