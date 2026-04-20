import os, json, requests
from datetime import datetime, timezone, timedelta

TOKEN = os.environ['HUBSPOT_TOKEN']
NB_PIPE = '57c08100-c4cd-4a1e-a2ac-e90519f45454'
RN_PIPE = 'fe3127d4-2fac-46cc-af91-d21cd0ce70cd'

HEADERS = {'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'}

def ms_to_iso(ms):
    if not ms: return None
    try: return datetime.fromtimestamp(int(ms)/1000, tz=timezone.utc).isoformat()
    except: return None

def ms_to_date(ms):
    if not ms: return None
    try: return datetime.fromtimestamp(int(ms)/1000, tz=timezone.utc).strftime('%Y-%m-%d')
    except: return None

def fetch_deals():
    resp = requests.post(
        'https://api.hubapi.com/crm/v3/objects/deals/search',
        headers=HEADERS,
        json={
            'filterGroups': [{'filters': [
                {'propertyName': 'pipeline', 'operator': 'IN', 'values': [NB_PIPE, RN_PIPE]},
                {'propertyName': 'hs_is_closed', 'operator': 'EQ', 'value': 'false'},
            ]}],
            'properties': [
                'dealname', 'amount', 'pipeline', 'dealstage', 'closedate',
                'hs_next_step', 'hs_lastmodifieddate',
                'hs_deal_stage_probability', 'hs_probability', 'num_associated_contacts',
            ],
            'sorts': [{'propertyName': 'amount', 'direction': 'DESCENDING'}],
            'limit': 100,
        }
    )
    resp.raise_for_status()
    deals = []
    for r in resp.json().get('results', []):
        p = r['properties']
        prob_raw = float(p.get('hs_deal_stage_probability') or p.get('hs_probability') or 20)
        prob = prob_raw if prob_raw <= 1 else prob_raw / 100
        last_act = p.get('hs_lastmodifieddate')
        deals.append({
            'id': r['id'],
            'name': p.get('dealname', ''),
            'amount': float(p.get('amount') or 0),
            'prob': round(prob, 2),
            'pipe': p.get('pipeline', ''),
            'stageId': p.get('dealstage', ''),
            'close': ms_to_date(p.get('closedate')),
            'nextStep': p.get('hs_next_step') or '',
            'lastAct': last_act[:10] if last_act else None,
            'contacts': int(p.get('num_associated_contacts') or 0),
        })
    return deals

def fetch_tasks():
    resp = requests.post(
        'https://api.hubapi.com/crm/v3/objects/tasks/search',
        headers=HEADERS,
        json={
            'filterGroups': [{'filters': [
                {'propertyName': 'hs_task_status', 'operator': 'NEQ', 'value': 'COMPLETED'},
            ]}],
            'properties': ['hs_task_subject', 'hs_task_due_date', 'hs_task_body'],
            'sorts': [{'propertyName': 'hs_task_due_date', 'direction': 'ASCENDING'}],
            'limit': 50,
        }
    )
    resp.raise_for_status()
    tasks = []
    for r in resp.json().get('results', []):
        p = r['properties']
        tasks.append({
            'id': r['id'],
            'subject': p.get('hs_task_subject', ''),
            'due': p.get('hs_task_due_date') or None,
            'body': p.get('hs_task_body') or '',
        })
    return tasks

def fetch_meetings():
    cutoff = int((datetime.now(timezone.utc) - timedelta(days=30)).timestamp() * 1000)
    resp = requests.post(
        'https://api.hubapi.com/crm/v3/objects/meetings/search',
        headers=HEADERS,
        json={
            'filterGroups': [{'filters': [
                {'propertyName': 'hs_meeting_start_time', 'operator': 'GTE', 'value': str(cutoff)},
            ]}],
            'properties': ['hs_meeting_title', 'hs_meeting_start_time', 'hs_meeting_end_time', 'hs_meeting_outcome'],
            'sorts': [{'propertyName': 'hs_meeting_start_time', 'direction': 'DESCENDING'}],
            'limit': 20,
        }
    )
    resp.raise_for_status()
    meetings = []
    for r in resp.json().get('results', []):
        p = r['properties']
        meetings.append({
            'id': r['id'],
            'title': p.get('hs_meeting_title') or 'Untitled Meeting',
            'start': ms_to_iso(p.get('hs_meeting_start_time')),
            'end': ms_to_iso(p.get('hs_meeting_end_time')),
            'outcome': p.get('hs_meeting_outcome'),
        })
    return meetings

def main():
    print('Fetching deals...')
    deals = fetch_deals()
    print(f'  {len(deals)} deals')

    print('Fetching tasks...')
    try:
        tasks = fetch_tasks()
        print(f'  {len(tasks)} tasks')
    except Exception as e:
        print(f'  Tasks unavailable: {e}')
        tasks = []

    print('Fetching meetings...')
    try:
        meetings = fetch_meetings()
        print(f'  {len(meetings)} meetings')
    except Exception as e:
        print(f'  Meetings unavailable: {e}')
        meetings = []

    output = {
        'synced': datetime.now(timezone.utc).isoformat(),
        'deals': deals,
        'tasks': tasks,
        'meetings': meetings,
    }
    with open('data.json', 'w') as f:
        json.dump(output, f, indent=2)
    print('data.json written.')

if __name__ == '__main__':
    main()
