/*
 * sensor_simulator.cpp
 * ---------------------
 * Simulates the firmware layer of an embedded sensor node (e.g. ESP32 +
 * DHT22 temperature/humidity sensor). Designed to mirror how a real device
 * would behave: fixed-interval polling, bounded jitter, occasional sensor
 * fault injection, and line-oriented JSON output over what would be a
 * serial/UART or WiFi socket connection on real hardware.
 *
 * On real hardware (ESP32 + Arduino framework) this loop maps almost
 * directly onto:
 *
 *     void loop() {
 *       float t = dht.readTemperature();
 *       float h = dht.readHumidity();
 *       Serial.println(buildJsonPayload(t, h));
 *       delay(POLL_INTERVAL_MS);
 *     }
 *
 * Here, stdout stands in for Serial.println() / a WiFi socket write, and
 * this binary is spawned as a subprocess by the ingestion service, which
 * is exactly the role a serial-port reader or MQTT broker plays in a real
 * deployment.
 *
 * Build:  g++ -O2 -std=c++17 sensor_simulator.cpp -o sensor_simulator
 * Run:    ./sensor_simulator [num_sensors] [interval_ms]
 */

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <random>
#include <string>
#include <thread>

struct SensorState {
    int id;
    double temperature_c;
    double humidity_pct;
    bool faulted = false;
};

// Simulates realistic sensor drift rather than pure random noise -
// each reading walks a small step from the previous one, like a real
// physical sensor tracking a slowly changing environment.
static double driftStep(std::mt19937& rng, double current, double min_v,
                         double max_v, double max_step) {
    std::uniform_real_distribution<double> step(-max_step, max_step);
    double next = current + step(rng);
    if (next < min_v) next = min_v;
    if (next > max_v) next = max_v;
    return next;
}

static std::string isoTimestamp() {
    auto now = std::chrono::system_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                  now.time_since_epoch()) % 1000;
    std::time_t t = std::chrono::system_clock::to_time_t(now);
    std::tm tm_utc{};
    gmtime_r(&t, &tm_utc);
    char buf[32];
    std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%S", &tm_utc);
    return std::string(buf) + "." + std::to_string(ms.count()) + "Z";
}

int main(int argc, char** argv) {
    int num_sensors = (argc > 1) ? std::atoi(argv[1]) : 3;
    int interval_ms = (argc > 2) ? std::atoi(argv[2]) : 1000;

    std::mt19937 rng(std::random_device{}());
    std::uniform_int_distribution<int> fault_roll(0, 999);

    std::vector<SensorState> sensors;
    for (int i = 0; i < num_sensors; ++i) {
        sensors.push_back({i, 24.0 + (i * 0.5), 45.0 + (i * 1.5), false});
    }

    // Flush stdout aggressively - a real serial/WiFi link doesn't buffer
    // for long, and the ingestion service downstream expects line-by-line
    // delivery, not a big blocking write at process exit.
    std::ios::sync_with_stdio(false);

    while (true) {
        for (auto& s : sensors) {
            // ~0.5% chance per tick of a transient sensor fault, matching
            // real-world flaky-sensor behavior (loose wire, brownout, etc).
            s.faulted = fault_roll(rng) < 5;

            if (!s.faulted) {
                s.temperature_c = driftStep(rng, s.temperature_c, -10, 55, 0.3);
                s.humidity_pct = driftStep(rng, s.humidity_pct, 0, 100, 0.8);
            }

            std::cout << "{"
                      << "\"sensor_id\":" << s.id << ","
                      << "\"timestamp\":\"" << isoTimestamp() << "\","
                      << "\"temperature_c\":" << (s.faulted ? -999.0 : s.temperature_c) << ","
                      << "\"humidity_pct\":" << (s.faulted ? -999.0 : s.humidity_pct) << ","
                      << "\"fault\":" << (s.faulted ? "true" : "false")
                      << "}" << std::endl;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
    }
    return 0;
}
