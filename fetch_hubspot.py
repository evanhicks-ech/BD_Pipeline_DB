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

def fetch_owner_map():
    resp = requests.get('https://api.hubapi.com/crm/v3/owners?limit=100', headers=HEADERS)
    resp.raise_for_status()
    owners = {}
    for o in resp.json().get('results', []):
        first = o.get('firstName') or ''
        last = o.get('lastName') or ''
        name = f"{first} {last}".strip() or 'Unknown'
        owners[str(o['id'])] = name
    return owners

def fetch_deals():
    owner_map = fetch_owner_map()
    resp = requests.post(
        'https://api.hubapi.com/crm/v3/objects/deals/search',
        headers=HEADERS,
        json={
            'filterGroups': [{'filters': [
                {'propertyName': 'pipeline', 'operator': 'IN', 'values': [NB_PIPE, RN_PIPE]},
                {'propertyName': 'hs_is_closed', 'operator': 'EQ', 'value': 'false'},
                {'propertyName': 'dealstage', 'operator': 'NEQ', 'value': '1269283354'},
            ]}],
            'properties': [
                'dealname', 'amount', 'pipeline', 'dealstage', 'closedate',
                'hs_next_step', 'notes_last_updated', 'hs_lastmodifieddate',
                'hs_deal_stage_probability', 'hs_probability', 'num_associated_contacts',
                'hubspot_owner_id', 'dealtype', 'description',
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
        last_act = p.get('notes_last_updated') or p.get('hs_lastmodifieddate')
        owner_id = str(p.get('hubspot_owner_id') or '')
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
            'owner': owner_map.get(owner_id, 'Unassigned'),
            'dealType': p.get('dealtype') or '',
            'description': p.get('description') or '',
        })
    return deals

def fetch_closed_lost():
    cutoff = int((datetime.now(timezone.utc) - timedelta(days=90)).timestamp() * 1000)
    owner_map = fetch_owner_map()
    resp = requests.post(
        'https://api.hubapi.com/crm/v3/objects/deals/search',
        headers=HEADERS,
        json={
            'filterGroups': [{'filters': [
                {'propertyName': 'dealstage', 'operator': 'IN', 'values': [
                    'caa876d5-e7c0-467b-9456-ec136d6f83f1',  # NB closed lost
                    '74889ee1-e828-43e0-a467-5e2916da3932',  # RN closed lost
                ]},
                {'propertyName': 'closedate', 'operator': 'GTE', 'value': str(cutoff)},
            ]}],
            'properties': ['dealname', 'amount', 'pipeline', 'dealtype', 'closedate', 'hubspot_owner_id', 'closed_lost_reason'],
            'sorts': [{'propertyName': 'closedate', 'direction': 'DESCENDING'}],
            'limit': 100,
        }
    )
    resp.raise_for_status()
    deals = []
    for r in resp.json().get('results', []):
        p = r['properties']
        owner_id = str(p.get('hubspot_owner_id') or '')
        deals.append({
            'id': r['id'],
            'name': p.get('dealname', ''),
            'amount': float(p.get('amount') or 0),
            'pipe': p.get('pipeline', ''),
            'dealType': p.get('dealtype') or '',
            'close': p.get('closedate', '')[:10] if p.get('closedate') else None,
            'owner': owner_map.get(owner_id, 'Unassigned'),
            'lostReason': p.get('closed_lost_reason') or '',
        })
    return deals

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

    print('Fetching closed lost...')
    try:
        closed_lost = fetch_closed_lost()
        print(f'  {len(closed_lost)} closed lost (last 90 days)')
    except Exception as e:
        print(f'  Closed lost unavailable: {e}')
        closed_lost = []

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
        'closedLost': closed_lost,
        'meetings': meetings,
    }
    with open('data.json', 'w') as f:
        json.dump(output, f, indent=2)
    print('data.json written.')

if __name__ == '__main__':
    main()
