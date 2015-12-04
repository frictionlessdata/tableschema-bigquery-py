# jsontableschema-bigquery-py

Generate and load BigQuery tables based on JSON Table Schema descriptors.

## Usage

### Authentification

To start using Google BigQuery service:
- Create a project - [link](https://console.developers.google.com/home/dashboard)
- Create a service key - [link](https://console.developers.google.com/apis/credentials)
- Add environment variables extracted from previous step json:
    - GOOGLE_CLIENT_EMAIL
    - GOOGLE_PRIVATE_KEY (for bash `export VAR=$'...'` to no not escape newlines)
