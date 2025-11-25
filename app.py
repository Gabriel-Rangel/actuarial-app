from flask import Flask, render_template, request, jsonify, request
from genie_embedding import new_genie_conversation, get_genie_space_id, get_databricks_oauth_token, continue_genie_conversation
from dotenv import load_dotenv
import os
import logging
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
from datetime import datetime
import copy

# Initialize Flask app
app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO)

# Pull Environment Variables
load_dotenv()
#databricks_host = "https://e2-demo-west.cloud.databricks.com"
databricks_host = os.environ['DATABRICKS_HOST']
workspace_id = os.environ['DATABRICKS_WORKSPACE_ID']

w = WorkspaceClient()

def extract_email(email):
    try:
        local_part = email.split('@')[0]
        first_name, last_name = local_part.split('.')
        return f"{first_name.capitalize()} {last_name.capitalize()}"
    except Exception:
        return local_part
    
def extract_first_name(email):
    try:
        local_part = email.split('@')[0]
        first_name, last_name = local_part.split('.')
        return f"{first_name.capitalize()}"
    except Exception:
        return local_part

# Function to call the model endpoint
def call_model_endpoint(endpoint_name, messages, max_tokens=300, timeout_minutes=3):
    chat_messages = [
        ChatMessage(
            content=message["content"],
            role=ChatMessageRole[message["role"].upper()]
        ) if isinstance(message, dict) else ChatMessage(content=message, role=ChatMessageRole.USER)
        for message in messages
    ]
    response = w.serving_endpoints.query(
        name=endpoint_name,
        messages=chat_messages,
        max_tokens=max_tokens
    )
    return response.choices[0].message.content

# Function to run the chain
def run_chain(question, answer, **kwargs):
    #formatted_prompt = prompt_template.format(**kwargs)
    clean_content = f"Your job is to use this answer: ({answer}). Use it to respond to this question:({question}). If the answer is too long or too much JSON, just read through it and give me a summary of the responses to the best of your ability. For example, just give one row in reasonable English and then stop."
    messages = [
        {"role": "system", "content": clean_content},
        {"role": "user", "content": clean_content}
    ]
    response = call_model_endpoint("databricks-meta-llama-3-1-8b-instruct", messages)
    return response

@app.route('/genie')
def home():
    forwarded_email = request.headers.get('X-Forwarded-Email')
    name = extract_email(forwarded_email)
    first_name = extract_first_name(forwarded_email)
    app = request.args.get('app', 'va')
    if app == 'va':
        title_name = 'Variable Annuity Valuation'
    else:
        title_name = 'Long Term Care Incidence'
    return render_template('genie.html', user=name, first_name=first_name, app=app, title_name=title_name)

@app.route('/analytics')
@app.route('/')
def analytics():
    """
    Render the Analytics page with an embedded Databricks dashboard.
    """
    # Pass the dashboard URL to the template for embedding
    app = request.args.get('app', 'va')  # Get app from query params, default to 'va'
    if app == 'va':
        dashboard_id = os.environ['DATABRICKS_DASHBOARD_VA_ID']
        dashboard_name = 'Variable Annuity Valuation'
    else:
        dashboard_id = os.environ['DATABRICKS_DASHBOARD_LTC_ID']
        dashboard_name = 'Long Term Care Incidence'
    dashboard_url = f"{databricks_host}/embed/dashboardsv3/{dashboard_id}?o={workspace_id}"
    forwarded_user = request.headers.get('X-Forwarded-Preferred-Username')
    return render_template('analytics.html', dashboard_url=dashboard_url, 
                           user=extract_email(forwarded_user), first_name=extract_first_name(forwarded_user),
                           dashboard_name = dashboard_name)

@app.route('/api/openai/chat', methods=['POST'])
def openai_chat():
    """
    Handle chat requests using OpenAI's Chat Completion API.
    """
    data = request.json  # Get JSON payload from frontend
    user_message = data.get('question', '')  # User's input message

    if not user_message:
        return jsonify({"error": "No message provided"}), 400
    try:
        ai_message = run_chain(user_message)
        return jsonify({"content": ai_message})  # Send AI response back to frontend
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/genie/start_conversation', methods=['POST'])
def genie_start_conversation():
    """
    Start a new conversation with Genie.
    """
    data = request.json  # Get JSON payload from the frontend

    question = data.get('question', '')
    app = data.get('app', '')

    if not question:
        return jsonify({"error": "No question provided"}), 400

    try:
        # Log the incoming request
        logging.info(f"Received question: {question}")

        # Get Databricks OAuth token
        token = get_databricks_oauth_token()
        logging.info("Obtained Databricks OAuth token")

        # Get Genie Space ID for the user's company
        space_id = get_genie_space_id(app)
        logging.info(f"Using app: {app}")
        logging.info(f"Using Genie Space ID: {space_id}")

        # Start a new conversation with Genie
        databricks_host = os.environ['DATABRICKS_HOST']
        first_response = new_genie_conversation(
            space_id=space_id,
            content=question,
            token=token,
            databricks_host=databricks_host
        )

        # Log the response from Genie
        logging.info(f"Genie response: {first_response}")
        response = run_chain(question=question, answer=first_response['query_result'])
        return jsonify(response)

    except Exception as e:
        logging.error(f"Error starting conversation with Genie: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/genie/continue_conversation', methods=['POST'])
def genie_continue_conversation():
    data = request.get_json()
    followup_message = data.get('question', '')
    app = data.get('app', '')
    conversation_id = data.get('conversation_id', '')
    databricks_token = data.get('databricks_token', '')

    if not followup_message or not conversation_id or not databricks_token:
        return jsonify({"error": "Missing required parameters"}), 400

    try:
        # Get Genie Space ID for the user's company
        databricks_genie_space_id = get_genie_space_id(app)
        logging.info(f"Using Genie Space ID: {databricks_genie_space_id}")

        # Continue the conversation with Genie
        response = continue_genie_conversation(
            space_id=databricks_genie_space_id,
            content=followup_message,
            conversation_id=conversation_id,
            token=databricks_token,
            # databricks_host="https:/databricks_host.databricks.com"
            databricks_host=databricks_host
        )
        # Log the response from Genie
        logging.info(f"Genie response: {response}")
        return jsonify(response)  # Return Genie's response as JSON
    except Exception as e:
        logging.error(f"Error continuing conversation with Genie: {e}")
        return jsonify({"error": str(e)}), 500

model_payoff = [
    {"productType": "DBIB", "Payoffs": "{:,}".format(19794000000)},
    {"productType": "DBRP", "Payoffs": "{:,}".format(3435000000)},
    {"productType": "DBRU", "Payoffs": "{:,}".format(-4000000)},
    {"productType": "DBWB", "Payoffs": "{:,}".format(3191000000)},
    {"productType": "IBRP", "Payoffs": "{:,}".format(10000000)},
    {"productType": "IBSU", "Payoffs": "{:,}".format(95000000)},
    {"productType": "MBRP", "Payoffs": "{:,}".format(8000000)},
    {"productType": "WBRP", "Payoffs": "{:,}".format(17000000)},
    {"productType": "WBRU", "Payoffs": "{:,}".format(9000000)},
    {"productType": "WBSU", "Payoffs": "{:,}".format(28000000)}
]

review_log = []

@app.route('/approval')
def approval():
    forwarded_user = request.headers.get('X-Forwarded-Preferred-Username')
    full_name = extract_email(forwarded_user)
    final_data = copy.deepcopy(model_payoff)
    for item in final_data:
        item['Payoffs'] = ""
    return render_template('approval.html', data=model_payoff, final_data=final_data, review_log=review_log, user=full_name, user_name=full_name)

@app.route('/submit_approval', methods=['POST'])
def submit_approval():
    global review_log
    final_data = copy.deepcopy(model_payoff)
    comments = request.form.get('comments')
    action = request.form.get('action')
    forwarded_user = request.headers.get('X-Forwarded-Preferred-Username')
    full_name = extract_email(forwarded_user)
    for item in model_payoff:
        product_type = item['productType']
        adj = request.form.get(f'adj_{product_type}')
        comment = request.form.get(f'comments_{product_type}')
        
        for final_item in final_data:
            if final_item['productType'] == product_type:
                final_item['Modeled'] = item['Payoffs']
                final_item['adj'] = "{:,}".format(int(adj))
                final_item['Payoffs'] = "{:,}".format(int(adj.replace(",", "")) + int(item['Payoffs'].replace(",", "")))
    review_log.append({
        'date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'user_name': full_name,
        'action': action,
        'comments': comments
    })
    return render_template('approval.html', data=model_payoff, final_data=final_data, review_log=review_log, user=full_name, user_name=full_name)

base_incidence_old = [
    {'Gender': 'Female',
     '0-49': 0.6,
     '50-54': 0.6,
     '55-59': 0.6,
     '60-64': 0.6,
     '65-69': 0.8,
    '70-74': 2.5,
    '75-79': 6.5,
    '80-84': 16.0,
    '85-89': 30.0,
    '90+': 45.0},
    {'Gender': 'Male',
     '0-49': 0.4,
     '50-54': 0.4,
     '55-59': 0.4,
     '60-64': 0.4,
     '65-69': 0.7,
    '70-74': 1.6,
    '75-79': 5.9,
    '80-84': 13.5,
    '85-89': 26.0,
    '90+': 44.0}
]

base_incidence = [
    {'Gender': 'Female',
    '0-49': 0.3249,
    '50-54': 0.3048,
    '55-59': 0.6793,
    '60-64': 0.5342,
    '65-69': 0.7823,
    '70-74': 2.0963,
    '75-79': 6.4849,
    '80-84': 15.7098,
    '85-89': 30.4289,
    '90+': 48.6443},
    {'Gender': 'Male',
     '0-49': 0.4714,
     '50-54': 0.4369,
     '55-59': 0.9839,
     '60-64': 0.3689,
     '65-69': 1.1339,
     '70-74': 2.9949,
     '75-79': 9.3339,
     '80-84': 22.3706,
     '85-89': 42.6649,
     '90+': 64.9651}
]

selection_fac = [
    {'Policy Duration': '1-3', 'Current Selection Factor': 0.6, 
     'Actual Selection Factor': 0.7, 'New Selection Factor': 0.7},
    {'Policy Duration': '4-6', 'Current Selection Factor': 0.8, 
     'Actual Selection Factor': 0.83, 'New Selection Factor': 0.83},
    {'Policy Duration': '7-9', 'Current Selection Factor': 0.9, 
     'Actual Selection Factor': 0.85, 'New Selection Factor': 0.85},
    {'Policy Duration': '10-12', 'Current Selection Factor': 1, 
     'Actual Selection Factor': 1.08, 'New Selection Factor': 1.08},
    {'Policy Duration': '13-15', 'Current Selection Factor': 1.1, 
     'Actual Selection Factor': 1.24, 'New Selection Factor': 1.24},
    {'Policy Duration': '15+', 'Current Selection Factor': 1.2, 
     'Actual Selection Factor': 1.7, 'New Selection Factor': 1.7}
]

other_assumptions = {'morb_imp': 0.012, 'marital_single': 1.2, 'marital_married': 0.85}
change_log = {'user': '',
              'action_base':'', 'comment_base':'', 'action_selection':'',
              'comment_selection':'',
              'action_imp':'', 'comment_imp':'', 'action_marital':'', 'comment_marital':''}

@app.route('/approval_ltc')
def approval_ltc():
    forwarded_user = request.headers.get('X-Forwarded-Preferred-Username')
    full_name = extract_email(forwarded_user)
    return render_template('approval_ltc.html', base_old=base_incidence_old, base_actual=base_incidence,
                           base_new = base_incidence, 
                           selection_fac=selection_fac, oth_assumptions = other_assumptions,
                           change_log=change_log, user=full_name, user_name=full_name)

@app.route('/submit_approval_ltc', methods=['POST'])
def submit_approval_ltc():
    forwarded_user = request.headers.get('X-Forwarded-Preferred-Username')
    full_name = extract_email(forwarded_user)
    for resp in ['action_base', 'comment_base', 'action_selection', 'comment_selection',
                 'action_imp', 'comment_imp', 'action_marital', 'comment_marital']:
        change_log[resp] = request.form.get(resp)
    change_log['user'] = full_name

    base_new = copy.deepcopy(base_incidence)
    for item in base_new:
        gender = item['Gender']
        item['0-49'] = request.form.get(f'base_0_49_{gender}')
        item['50-54'] = request.form.get(f'base_50_54_{gender}')
        item['55-59'] = request.form.get(f'base_55_59_{gender}')
        item['60-64'] = request.form.get(f'base_60_64_{gender}')
        item['65-69'] = request.form.get(f'base_65_69_{gender}')
        item['70-74'] = request.form.get(f'base_70_74_{gender}')
        item['75-79'] = request.form.get(f'base_75_79_{gender}')
        item['80-84'] = request.form.get(f'base_80_84_{gender}')
        item['85-89'] = request.form.get(f'base_85_89_{gender}')
        item['90+'] = request.form.get(f'base_90_{gender}')
    
    selection_new = copy.deepcopy(selection_fac)
    for item in selection_new:
        pol_dur = item['Policy Duration']
        item['New Selection Factor'] = request.form.get(f'selection_fac_new_{pol_dur}')
    
    other_assumptions_new = copy.deepcopy(other_assumptions)
    other_assumptions_new['marital_single'] = request.form.get('marital_new_single')
    other_assumptions_new['marital_married'] = request.form.get('marital_new_married')
    other_assumptions_new['morb_imp'] = request.form.get('morb_new')

    
    return render_template('approval_ltc.html', scroll_to='changes_table', 
                           base_old=base_incidence_old, base_actual=base_incidence, 
                           base_new = base_new,
                           selection_fac=selection_new, oth_assumptions = other_assumptions_new,
                           change_log=change_log, user=full_name, user_name=full_name)


if __name__ == '__main__':
    app.run(debug=True)
