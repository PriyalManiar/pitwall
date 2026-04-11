from ingestion import lap_times, weather, pit_stops, results, telemetry, f1db

def main():
    print("Running all ingestion scripts...")

    print("\n[1/6] Lap times")
    lap_times.run()

    print("\n[2/6] Weather")
    weather.run()

    print("\n[3/6] Pit stops")
    pit_stops.run()

    print("\n[4/6] Results")
    results.run()

    print("\n[5/6] Telemetry")
    telemetry.run()

    print("\n[6/6] F1DB historical")
    f1db.run()

    print("\nAll ingestion complete.")

if __name__ == "__main__":
    main()