import functions_framework
import requests
import pandas as pd
import json
import os

@functions_framework.http
def get_mandi_prices_handler(request):
    """
    An HTTP Cloud Function that acts as a tool for a Vertex AI Agent.
    It fetches mandi prices based on parameters sent by the agent.
    """
    # It's best practice to get API keys from environment variables
    # We will set this up in Cloud Run later.
    api_key = os.environ.get("DATA_GOV_API_KEY")
    if not api_key:
        # A fallback for testing, but not recommended for production
        api_key = "YOUR_API_KEY_HERE" # Replace if not using environment variables

    RESOURCE_ID = "9ef84268-d588-465a-a308-a864a43d0070"
    BASE_URL = f"https://api.data.gov.in/resource/{RESOURCE_ID}"

    try:
        request_json = request.get_json(silent=True)
        call_args = request_json['call']['arguments']
        state = call_args.get('state')
        district = call_args.get('district')
        commodity = call_args.get('commodity')
    except (TypeError, KeyError):
        return {"error": "Invalid request format. Expected Vertex AI tool call format."}, 400

    if not all([state, district, commodity]):
        return {"error": "Missing required parameters: state, district, or commodity."}, 400

    params = {
        "api-key": api_key,
        "format": "json",
        "limit": 10,
        "filters[state]": state,
        "filters[district]": district,
        "filters[commodity]": commodity,
    }

    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        data = response.json()
        records = data.get("records", [])

        if not records:
            result_text = f"No recent mandi price data found for {commodity} in {district}, {state}."
        else:
            df = pd.DataFrame(records)
            latest_record = df.iloc[0]
            result_text = (
                f"Latest price for {latest_record['commodity']} ({latest_record['variety']}) "
                f"in {latest_record['market']} market on {latest_record['arrival_date']}: "
                f"Modal Price is ₹{latest_record['modal_price']} per Quintal."
            )

        return json.dumps({"result": result_text})

    except requests.exceptions.RequestException as e:
        return json.dumps({"error": f"Failed to call external API: {e}"})
    except Exception as e:
        return json.dumps({"error": f"An unexpected error occurred: {e}"})