void setup() {
  // put your setup code here, to run once:
  pinMode(A0, INPUT);
  Serial.begin(9600);
  Serial.println("Sensor Activated...");
}

void loop() {
  // put your main code here, to run repeatedly:
  float pulse;
  int sum = 0;
  for (int i = 0; i < 20; i++)
    sum += analogRead(A0);
  pulse = sum / 20.00;
  Serial.println("Pulse rate: "+String(pulse));
  delay(100);
}
