# Architecture

<div aling="center">
  <img src="images/architecture_image.png" height="300" width="1600">
</div>

### Prerequisites - Start Events
1. Clone the repo
   ```
   git clone https://github.com/phillipefs/credit-card-streaming-analytics.git
   ```
2. Create .env file
   ```
   BATCH_QUANTITY=5
   BATCH_SIZE=100
   SLEEP_SECONDS=5
   CNN_STRING_EVENTHUB='Endpoint=sb:xxx'
   NAME_EVENTHUB="xxxxxxxxxxxxxxxxxxx"
   ```
3. Install requirements file`
   ```
   pip install -r requirements.txt
   ```
4. Run app_events`
   ```
   python .\app_events.py
   ```
