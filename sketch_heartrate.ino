#define samp_siz 4
#define rise_threshold 5
// Pulse Monitor Test Script
int sensorPin = 0;
int led = 13; // the pin that the LED is atteched to
int state = LOW; // default state of LED
float reader = 0.0;
void setup() {
   pinMode(led, OUTPUT); // initalize LED as an output
   digitalWrite(led,LOW);
   Serial.begin(9600); 
}
void loop ()
{    
  float pulse;
  int sum = 0;
  for (int i = 0; i < 20; i++)
    sum += analogRead(A0);
  pulse = sum / 200;
   if (pulse >= 100.0) {
     for (int i = 0; i <=5; i++) {
          digitalWrite(led,HIGH);
          delay(50);
          digitalWrite(led,LOW);
     } 
     
   } else {
      if (pulse == 3.00) {
        digitalWrite(led,LOW); //HIGH is set to about 5V PIN8
        delay(100);
      } else {
        digitalWrite(led,HIGH); //HIGH is set to about 5V PIN8
        delay(100);
      }
  }
  Serial.println(pulse);
  digitalWrite(led,LOW);
  delay(1000);
}