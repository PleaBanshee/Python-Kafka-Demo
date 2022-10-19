#define samp_siz 4
#define rise_threshold 5
// Pulse Monitor Test Script
int sensorPin = 0;
int led = 13; // the pin that the LED is atteched to
int state = LOW; // default state of LED
void setup() {
   pinMode(led, OUTPUT); // initalize LED as an output
   digitalWrite(led,LOW);
   Serial.begin(9600); 
}
void loop ()
{
   float reads[samp_siz], sum;
   long int now, ptr;
   float last, reader, start;
   float first, second, third, before, print_value;
   bool rising;
   int rise_count;
   int n;
   long int last_beat;
   for (int i = 0; i < samp_siz; i++)
     reads[i] = 0;
   sum = 0;
   ptr = 0;
   while(1)
   {
     // calculate an average of the sensor
     // during a 20 ms period (this will eliminate
     // the 50 Hz noise caused by electric light
     n = 0;
     start = millis();
     reader = 0.;
     do
     {
       reader += analogRead (sensorPin);
       n++;
       now = millis();
     }
     while (now < start + 20);  
     reader /= n;  // we got an average
     // Add the newest measurement to an array
     // and subtract the oldest measurement from the array
     // to maintain a sum of last measurements
     sum -= reads[ptr];
     sum += reader;
     reads[ptr] = reader;
     last = sum / samp_siz;
     // now last holds the average of the values in the array
     // check for a rising curve (= a heart beat)
     if (last > before)
     {
       rise_count++;
       if (!rising && rise_count > rise_threshold)
       {
         // Ok, we have detected a rising curve, which implies a heartbeat.
         // Record the time since last beat, keep track of the two previous
         // times (first, second, third) to get a weighed average.
         // The rising flag prevents us from detecting the same rise 
         // more than once.
         rising = true;
         first = millis() - last_beat;
         last_beat = millis();
         // Calculate the weighed average of heartbeat rate
         // according to the three last beats
         print_value = 60000. / (0.4 * first + 0.3 * second + 0.3 * third);
         if (print_value >= 100.0) {
           for (int i = 0; i <=5; i++) {
            digitalWrite(led,HIGH);
            delay(50);
            digitalWrite(led,LOW);
           } 
         } else {
            digitalWrite(led,HIGH); //HIGH is set to about 5V PIN8
         }
         Serial.print(print_value);
         Serial.print('\n');
         third = second;
         second = first;
       }
     }
     else
     {
       // Ok, the curve is falling
       // delay(500);
       digitalWrite(led,LOW);  //LOW is set to about 5V PIN8
       rising = false;
       rise_count = 0;
     }
     before = last;
     ptr++;
     ptr %= samp_siz;
   }
} 