from ingestion import lap_times, weather, pit_stops, results, telemetry

def main():
    print("Running all ingestion scripts...")
    
    print("\n[1/5] Lap times")
    lap_times.run()
    
    print("\n[2/5] Weather")
    weather.run()
    
    print("\n[3/5] Pit stops")
    pit_stops.run()
    
    print("\n[4/5] Results")
    results.run()
    
    print("\n[5/5] Telemetry")
    telemetry.run()
    
    print("\nAll FastF1 ingestion complete.")

if __name__ == "__main__":
    main()