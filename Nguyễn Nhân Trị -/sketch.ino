

#include <Arduino.h>
#include <WiFi.h>
#include <Wire.h>
#include <Adafruit_BMP085.h>
#include <PubSubClient.h>

// ===================== CAU HINH =====================
#define PIN_SDA     21
#define PIN_SCL     22
#define PIN_LED     18
#define PIN_BUZZER  19

// WiFi (Wokwi dung mang ao "Wokwi-GUEST")
const char* WIFI_SSID = "Wokwi-GUEST";
const char* WIFI_PASS = "";

// MQTT broker cong cong
const char* MQTT_HOST = "broker.hivemq.com";
const int   MQTT_PORT = 1883;

// Cac topic (doi "vietdinh" thanh chuoi rieng de tranh trung nguoi khac)
#define TOPIC_TEMP      "runner/vietdinh/temperature"
#define TOPIC_PRESSURE  "runner/vietdinh/pressure"
#define TOPIC_ALT       "runner/vietdinh/altitude"
#define TOPIC_JSON      "runner/vietdinh/data"
#define TOPIC_ALERT     "runner/vietdinh/alert"
#define TOPIC_CMD_LED   "runner/vietdinh/cmd/led"      // payload: "1"/"0"
#define TOPIC_CMD_BUZ   "runner/vietdinh/cmd/buzzer"   // payload: "1"/"0"

// Nguong canh bao
const float TEMP_HIGH       = 35.0;   // do C, qua nong
const float TEMP_LOW        = 15.0;   // do C, qua lanh
const float ALT_JUMP_LIMIT  = 5.0;    // m, thay doi do cao dot ngot giua 2 lan do
const float SEA_LEVEL_PA    = 101325; // Pa, ap suat tham chieu muc nuoc bien

const uint32_t SAMPLE_PERIOD_MS = 1000;

// ===================== KIEU DU LIEU =====================
struct SensorData {            // goi vao "data Queue" (day du)
  float temperature;           // do C
  float pressure;              // hPa
  float altitude;              // m
};

struct AlertData {             // goi vao "data Alert" (chi nhiet do + do cao)
  float temperature;
  float altitude;
};

// ===================== DOI TUONG RTOS =====================
QueueHandle_t     dataQueue;   // TaskReadSensor -> TaskTransmit
QueueHandle_t     alertQueue;  // TaskReadSensor -> TaskAlert
SemaphoreHandle_t serialMutex; // bao ve Serial khi nhieu task cung in

// Trang thai dieu khien manual (ghi boi Core 0 qua MQTT, doc boi Core 1)
volatile bool manualLed    = false;
volatile bool manualBuzzer = false;

Adafruit_BMP085 bmp;
WiFiClient      wifiClient;
PubSubClient    mqtt(wifiClient);

// ===================== TIEN ICH IN SERIAL AN TOAN =====================
void logf(const char* fmt, ...) {
  char buf[200];
  va_list args;
  va_start(args, fmt);
  vsnprintf(buf, sizeof(buf), fmt, args);
  va_end(args);
  if (xSemaphoreTake(serialMutex, pdMS_TO_TICKS(200)) == pdTRUE) {
    Serial.print(buf);
    xSemaphoreGive(serialMutex);
  }
}

// ===================== MQTT CALLBACK (chay tren Core 0) =====================
void mqttCallback(char* topic, byte* payload, unsigned int len) {
  bool on = (len > 0 && (payload[0] == '1' || payload[0] == 'O' || payload[0] == 'o'));
  if (strcmp(topic, TOPIC_CMD_LED) == 0) {
    manualLed = on;
    logf("[Core %d][MQTT] Lenh LED manual: %s\n", xPortGetCoreID(), on ? "ON" : "OFF");
  } else if (strcmp(topic, TOPIC_CMD_BUZ) == 0) {
    manualBuzzer = on;
    logf("[Core %d][MQTT] Lenh LOA manual: %s\n", xPortGetCoreID(), on ? "ON" : "OFF");
  }
}

// ===================== TASK 1: DOC DU LIEU (CORE 1) =====================
void TaskReadSensor(void* pv) {
  TickType_t lastWake = xTaskGetTickCount();
  logf("[Core %d] TaskReadSensor started\n", xPortGetCoreID());

  for (;;) {
    SensorData s;
    s.temperature = bmp.readTemperature();
    s.pressure    = bmp.readPressure() / 100.0f;           // Pa -> hPa
    s.altitude    = bmp.readAltitude(SEA_LEVEL_PA);

    // 1) Hien thi qua Serial
    logf("[Core %d][Read ] T=%.2f C | P=%.2f hPa | Alt=%.2f m\n",
         xPortGetCoreID(), s.temperature, s.pressure, s.altitude);

    // 2) Day du lieu day du vao data Queue (cho Task truyen)
    if (xQueueSend(dataQueue, &s, 0) != pdTRUE) {
      logf("[Core %d][Read ] dataQueue day, bo qua mau\n", xPortGetCoreID());
    }

    // 3) Day du lieu canh bao (nhiet do + do cao) vao data Alert
    AlertData a = { s.temperature, s.altitude };
    if (xQueueSend(alertQueue, &a, 0) != pdTRUE) {
      logf("[Core %d][Read ] alertQueue day, bo qua mau\n", xPortGetCoreID());
    }

    // Chu ky chinh xac 1s
    vTaskDelayUntil(&lastWake, pdMS_TO_TICKS(SAMPLE_PERIOD_MS));
  }
}

// ===================== TASK 2: TRUYEN DATA (CORE 0) =====================
void connectWiFi() {
  logf("[Core %d][Net  ] Dang ket noi WiFi %s ...\n", xPortGetCoreID(), WIFI_SSID);
  WiFi.mode(WIFI_STA);
  WiFi.begin(WIFI_SSID, WIFI_PASS, 6);
  while (WiFi.status() != WL_CONNECTED) {
    vTaskDelay(pdMS_TO_TICKS(500));
  }
  logf("[Core %d][Net  ] WiFi OK, IP=%s\n", xPortGetCoreID(), WiFi.localIP().toString().c_str());
}

bool connectMQTT() {
  String clientId = "esp32-runner-" + String((uint32_t)ESP.getEfuseMac(), HEX);
  if (mqtt.connect(clientId.c_str())) {
    mqtt.subscribe(TOPIC_CMD_LED);
    mqtt.subscribe(TOPIC_CMD_BUZ);
    logf("[Core %d][Net  ] MQTT OK (%s)\n", xPortGetCoreID(), MQTT_HOST);
    return true;
  }
  logf("[Core %d][Net  ] MQTT loi, rc=%d\n", xPortGetCoreID(), mqtt.state());
  return false;
}

void TaskTransmit(void* pv) {
  logf("[Core %d] TaskTransmit started\n", xPortGetCoreID());

  connectWiFi();                       // buoc 1: ket noi WiFi
  mqtt.setServer(MQTT_HOST, MQTT_PORT);
  mqtt.setCallback(mqttCallback);

  for (;;) {
    // Mat WiFi -> ket noi lai
    if (WiFi.status() != WL_CONNECTED) connectWiFi();

    // Mat MQTT -> ket noi lai
    if (!mqtt.connected()) {
      if (!connectMQTT()) {
        vTaskDelay(pdMS_TO_TICKS(2000));
        continue;
      }
    }

    // buoc 2: doc tu data Queue, cho toi da 100ms (task bi block, nhuong CPU)
    SensorData s;
    if (xQueueReceive(dataQueue, &s, pdMS_TO_TICKS(100)) == pdTRUE) {
      char buf[16];

      dtostrf(s.temperature, 1, 2, buf);  mqtt.publish(TOPIC_TEMP, buf);
      dtostrf(s.pressure,    1, 2, buf);  mqtt.publish(TOPIC_PRESSURE, buf);
      dtostrf(s.altitude,    1, 2, buf);  mqtt.publish(TOPIC_ALT, buf);

      char json[128];
      snprintf(json, sizeof(json),
               "{\"temperature\":%.2f,\"pressure\":%.2f,\"altitude\":%.2f}",
               s.temperature, s.pressure, s.altitude);
      mqtt.publish(TOPIC_JSON, json);

      logf("[Core %d][Send ] Da gui MQTT: %s\n", xPortGetCoreID(), json);
    }

    mqtt.loop();                          // xu ly lenh manual tu Web/App
  }
}

// ===================== TASK 3: CANH BAO (CORE 1) =====================
void TaskAlert(void* pv) {
  logf("[Core %d] TaskAlert started\n", xPortGetCoreID());

  float prevAlt    = NAN;
  bool  autoAlert  = false;
  bool  blinkState = false;
  const char* reason = "OK";

  for (;;) {
    // Cho du lieu canh bao toi da 150ms; het thoi gian van chay tiep de nhap nhay
    AlertData a;
    if (xQueueReceive(alertQueue, &a, pdMS_TO_TICKS(150)) == pdTRUE) {
      bool tooHot   = a.temperature > TEMP_HIGH;
      bool tooCold  = a.temperature < TEMP_LOW;
      bool altJump  = !isnan(prevAlt) && fabsf(a.altitude - prevAlt) > ALT_JUMP_LIMIT;
      prevAlt = a.altitude;

      autoAlert = tooHot || tooCold || altJump;
      reason = tooHot ? "NHIET DO CAO" : tooCold ? "NHIET DO THAP"
             : altJump ? "DO CAO THAY DOI DOT NGOT" : "OK";

      logf("[Core %d][Alert] T=%.2f Alt=%.2f -> %s\n",
           xPortGetCoreID(), a.temperature, a.altitude, reason);
    }

    // Dieu khien Led / Loa: manual (tu Web) hoac tu dong (canh bao)
    blinkState = !blinkState;                       // nhap nhay ~3Hz
    bool ledOn = manualLed    || (autoAlert && blinkState);
    bool bzOn  = manualBuzzer || (autoAlert && blinkState);

    digitalWrite(PIN_LED, ledOn ? HIGH : LOW);
    if (bzOn) tone(PIN_BUZZER, 2000); else noTone(PIN_BUZZER);
  }
}

// ===================== SETUP / LOOP =====================
void setup() {
  Serial.begin(115200);
  pinMode(PIN_LED, OUTPUT);
  pinMode(PIN_BUZZER, OUTPUT);
  digitalWrite(PIN_LED, LOW);

  Wire.begin(PIN_SDA, PIN_SCL);
  if (!bmp.begin()) {
    Serial.println("Khong tim thay BMP180! Kiem tra day SDA/SCL.");
    while (true) delay(1000);
  }

  serialMutex = xSemaphoreCreateMutex();
  dataQueue   = xQueueCreate(10, sizeof(SensorData));   // "data Queue"
  alertQueue  = xQueueCreate(10, sizeof(AlertData));    // "data Alert"

  if (!dataQueue || !alertQueue || !serialMutex) {
    Serial.println("Khong tao duoc doi tuong RTOS!");
    while (true) delay(1000);
  }

  // xTaskCreatePinnedToCore(ham, ten, stack, tham so, uu tien, handle, core)
  xTaskCreatePinnedToCore(TaskTransmit,   "TaskTransmit",   8192, NULL, 1, NULL, 0); // Core 0
  xTaskCreatePinnedToCore(TaskReadSensor, "TaskReadSensor", 4096, NULL, 2, NULL, 1); // Core 1
  xTaskCreatePinnedToCore(TaskAlert,      "TaskAlert",      4096, NULL, 3, NULL, 1); // Core 1
}

void loop() {
  // Moi cong viec da nam trong cac task RTOS; loop() nhuong CPU.
  vTaskDelay(portMAX_DELAY);
}
