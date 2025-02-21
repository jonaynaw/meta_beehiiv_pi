import os
import json
import time
import logging
import psycopg2
import requests
from datetime import datetime, timedelta, timezone
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.business import Business
from facebook_business.exceptions import FacebookRequestError

def create_db_connection():
    try:
        connection = psycopg2.connect(
            host=os.environ["DB_HOST"],
            database=os.environ["DB_DATABASE"],
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASSWORD"],
            port=os.environ["DB_PORT"]
        )
        cursor = connection.cursor()
        return connection, cursor
    except Exception as e:
        logging.error(f"Error connecting to database: {str(e)}")
        raise

def fetch_data_from_facebook_api(effective_status=['ACTIVE']):
    app_id = os.environ["FACEBOOK_APP_ID"]
    app_secret = os.environ["FACEBOOK_APP_SECRET"]
    access_token = os.environ["FACEBOOK_ACCESS_TOKEN"]
    business_id = os.environ["FACEBOOK_BUSINESS_ID"]

    FacebookAdsApi.init(app_id, app_secret, access_token)

    status_map = {
        1: 'ACTIVE',
        2: 'DISABLED',
        3: 'UNSETTLED',
        7: 'PENDING_RISK_REVIEW',
        8: 'PENDING_SETTLEMENT',
        9: 'IN_GRACE_PERIOD',
        100: 'PENDING_CLOSURE',
        101: 'CLOSED',
        201: 'ANY_ACTIVE',
        202: 'ANY_CLOSED'
    }

    def check_limit(response, use_response=True, account_number=None):
        ad_account_usage = 0
        business_usage = 0
        app_usage = 0

        if not use_response:
            response = requests.get(f'https://graph.facebook.com/v20.0/act_{account_number}/insights?access_token={access_token}')
        
        headers = response._headers if hasattr(response, '_headers') else response.headers if not use_response else response._http_headers

        if 'x-ad-account-usage' in headers:
            ad_account_usage_data = json.loads(headers['x-ad-account-usage'])
            ad_account_usage = float(ad_account_usage_data.get('acc_id_util_pct', 0))
            reset_time_duration = ad_account_usage_data.get('reset_time_duration', 0)
        
        if 'x-business-use-case-usage' in headers:
            business_usage_data = json.loads(headers['x-business-use-case-usage'])
            for key, value in business_usage_data.items():
                account_usage = value[0]
                business_call = float(account_usage['call_count'])
                business_cpu = float(account_usage['total_cputime'])
                business_total = float(account_usage['total_time'])
                estimated_time_to_regain_access = account_usage.get('estimated_time_to_regain_access', 0)
                ads_api_access_tier = account_usage.get('ads_api_access_tier', 'UNKNOWN')
                type = account_usage.get('type', 'UNKNOWN')
                business_usage = max(business_call, business_cpu, business_total)

        if 'x-fb-ads-insights-throttle' in headers:
            insights_throttle_data = json.loads(headers['x-fb-ads-insights-throttle'])
            app_usage = float(insights_throttle_data.get('app_id_util_pct', 0))
            ad_account_usage = max(ad_account_usage, float(insights_throttle_data.get('acc_id_util_pct', 0)))
        
        if 'x-app-usage' in headers:
            app_usage_data = json.loads(headers['x-app-usage'])
            app_usage = max(app_usage, float(app_usage_data.get('call_count', 0)))

        return max(ad_account_usage, business_usage, app_usage)

    def api_call_with_retries(func, *args, **kwargs):
        max_retries = 5
        retry_count = 0
        while retry_count < max_retries:
            try:
                response = func(*args, **kwargs)
                usage = check_limit(response)
                if usage >= 100:
                    sleep_time = 60 * (2 ** retry_count)
                    time.sleep(sleep_time)
                    retry_count += 1
                    continue
                return response
            except FacebookRequestError as e:
                if e.api_error_code() in [17, 80004]:
                    sleep_time = 60 * (2 ** retry_count)
                    time.sleep(sleep_time)
                    retry_count += 1
                else:
                    raise
        raise Exception("Max retries exceeded")

    def get_time_range(init_date):
        return {
            'since': init_date,
            'until': datetime.now().strftime('%Y-%m-%d')
        }

    def get_ad_accounts_with_insights(business_id, init_date, add_insights=False):
        business = Business(business_id)
        
        fields = ['id', 'name', 'currency', 'timezone_name', 'created_time']
        params = {
            'time_range': get_time_range(init_date),
            'level': 'account'
        }

        insights_fields = ['spend', 'clicks', 'unique_clicks', 'cpc', 'ctr', 'impressions', 'reach']
        insights_params = {
            'location': {'breakdowns': ['region', 'country'], 'time_range': get_time_range(init_date)},
            'audience': {'breakdowns': ['age', 'gender'], 'time_range': get_time_range(init_date)}
        }

        ad_accounts = list(api_call_with_retries(business.get_owned_ad_accounts, fields=fields))
        ad_accounts_info = []

        for ad_account in ad_accounts:
            ad_account_data = ad_account.export_all_data()
            if add_insights:
                for key, value in insights_params.items():
                    insights = api_call_with_retries(ad_account.get_insights, fields=insights_fields, params=value)
                    if insights:
                        ad_account_data[f'insights_{key}'] = [insight.export_all_data() for insight in insights]
            ad_accounts_info.append(ad_account_data)

        return ad_accounts, ad_accounts_info

    def get_campaigns_with_insights(ad_account, init_date, effective_status=['ACTIVE'], add_insights=False):
        fields = ['id', 'name', 'objective', 'status', 'created_time', 'start_time', 'stop_time', 'daily_budget', 'lifetime_budget']
        params = {
            'effective_status': effective_status,
            'time_range': get_time_range(init_date),
            'level': 'campaign'
        }

        insights_fields = ['spend', 'clicks', 'unique_clicks', 'cpc', 'ctr', 'impressions', 'reach']
        insights_params = {
            'location': {'breakdowns': ['region', 'country'], 'time_range': get_time_range(init_date)},
            'audience': {'breakdowns': ['age', 'gender'], 'time_range': get_time_range(init_date)}
        }

        campaigns = list(api_call_with_retries(ad_account.get_campaigns, fields=fields, params=params))
        campaigns_info = []

        for campaign in campaigns:
            campaign_data = campaign.export_all_data()
            if add_insights:
                for key, value in insights_params.items():
                    insights = api_call_with_retries(campaign.get_insights, fields=insights_fields, params=value)
                    if insights:
                        campaign_data[f'insights_{key}'] = [insight.export_all_data() for insight in insights]
            campaigns_info.append(campaign_data)

        return campaigns, campaigns_info

    def get_ad_sets_with_insights(campaigns, init_date, add_insights=False):
        fields = [
            'id', 'campaign_id', 'name', 'status', 'created_time', 'start_time', 'stop_time',
            'daily_budget', 'lifetime_budget', 'bid_amount', 'bid_strategy', 'billing_event',
            'optimization_goal', 'targeting'
        ]
        params = {
            'time_range': get_time_range(init_date),
            'level': 'adset'
        }

        insights_fields = ['spend', 'clicks', 'unique_clicks', 'cpc', 'ctr', 'impressions', 'reach']
        insights_params = {
            'location': {'breakdowns': ['region', 'country'], 'time_range': get_time_range(init_date)},
            'audience': {'breakdowns': ['age', 'gender'], 'time_range': get_time_range(init_date)}
        }
        
        all_ad_sets, ad_sets_info = [], []

        for campaign in campaigns:
            ad_sets = list(api_call_with_retries(campaign.get_ad_sets, fields=fields, params=params))
            for ad_set in ad_sets:
                ad_set_data = ad_set.export_all_data()
                if add_insights:
                    for key, value in insights_params.items():
                        insights = api_call_with_retries(ad_set.get_insights, fields=insights_fields, params=value)
                        if insights:
                            ad_set_data[f'insights_{key}'] = [insight.export_all_data() for insight in insights]
                ad_sets_info.append(ad_set_data)
            all_ad_sets.extend(ad_sets)

        return all_ad_sets, ad_sets_info

    def get_ads_with_insights(ad_sets, init_date, add_insights=False):
        fields = ['id', 'adset_id', 'name', 'status', 'created_time']
        params = {
            'time_range': get_time_range(init_date),
            'level': 'ad'
        }

        insights_fields = ['spend', 'clicks', 'unique_clicks', 'cpc', 'ctr', 'impressions', 'reach']
        insights_params = {
            'location': {'breakdowns': ['region', 'country'], 'time_range': get_time_range(init_date)},
            'audience': {'breakdowns': ['age', 'gender'], 'time_range': get_time_range(init_date)}
        }

        ads_info = []
        for ad_set in ad_sets:
            ads = list(api_call_with_retries(ad_set.get_ads, fields=fields, params=params))
            for ad in ads:
                ad_data = ad.export_all_data()
                if add_insights:
                    for key, value in insights_params.items():
                        insights = api_call_with_retries(ad.get_insights, fields=insights_fields, params=value)
                        if insights:
                            ad_data[f'insights_{key}'] = [insight.export_all_data() for insight in insights]
                ads_info.append(ad_data)
        
        return ads_info

    init_date = '2024-01-01'
    ad_accounts, ad_accounts_info = get_ad_accounts_with_insights(business_id, init_date, add_insights=True)

    for n, ad_account in enumerate(ad_accounts):
        ad_account_campaigns, ad_account_campaigns_info = get_campaigns_with_insights(ad_account, init_date, effective_status, add_insights=True)
        ad_account_ad_sets, ad_account_ad_sets_info = get_ad_sets_with_insights(ad_account_campaigns, init_date, add_insights=True)
        ad_account_ads_info = get_ads_with_insights(ad_account_ad_sets, init_date, add_insights=True)

        for ad_campaign in ad_account_campaigns_info:
            ad_sets_filtered = list(filter(lambda x: x['campaign_id'] == ad_campaign['id'], ad_account_ad_sets_info))
            for ad_set in ad_sets_filtered:
                ads_filtered = list(filter(lambda x: x['adset_id'] == ad_set['id'], ad_account_ads_info))
                ad_set.update({'ads': ads_filtered})
            ad_campaign.update({'ad_sets': ad_sets_filtered})
        ad_accounts_info[n].update({'campaigns': ad_account_campaigns_info})

    return ad_accounts_info

def fetch_data_from_beehiiv_api():
    api_headers = {
        "Accept": "application/json",
        "Authorization": os.environ["BEEHIIV_API_KEY"],
    }

    pub_params = {
        "direction": "desc",
        "expand[]": "stats",
        "limit": "100",
        "order_by": "publish_date",
        "status": "confirmed"
    }

    segment_params = {
        "limit": "100",
    }

    def get_pubs_info():
        names = {}
        url = "https://api.beehiiv.com/v2/publications"
        response = requests.get(url, headers=api_headers, params={ "expand[]": "stats", "limit": "100", "order_by": "name" })
        response = response.json()
        for i in response['data']:
            names.update({ 
                i['name']: { 
                    'id': i['id'],
                    'name': i['name'],
                    'organization_name': i['organization_name'],
                    'publication_stats': {
                        'active_subscriptions': i['stats']['active_subscriptions'],
                        'active_premium_subscriptions': i['stats']['active_premium_subscriptions'],
                        'active_free_subscriptions': i['stats']['active_free_subscriptions'],
                        'average_open_rate': i['stats']['average_open_rate'],
                        'average_click_rate': i['stats']['average_click_rate'],
                        'total_sent': i['stats']['total_sent'],
                        'total_unique_opened': i['stats']['total_unique_opened'],
                        'total_clicked': i['stats']['total_clicked']
                    }
                }
            })
        return names

    def get_posts_info(pub_id, top_date, pub_name):
        posts_responses = []
        for i in range(1, 11):
            break_for = False
            pub_params["page"] = i
            posts_temp_response = requests.get(f"https://api.beehiiv.com/v2/publications/{pub_id}/posts", headers=api_headers, params=pub_params)
            posts_temp_response = posts_temp_response.json()
            for element in posts_temp_response['data']:
                post_data = {}
                publish_date = datetime.fromtimestamp(element['publish_date'])
                if publish_date > top_date:
                    post_data['publication_id'] = pub_id
                    post_data['publication_name'] = pub_name
                    post_data['publish_date'] = publish_date.strftime('%Y-%m-%d')
                    post_data['post_id'] = element['id']
                    post_data['delivered'] = element['stats']['email']['recipients']
                    post_data['clicks'] = element['stats']['email']['clicks']
                    post_data['unique_clicks'] = element['stats']['email']['unique_clicks']
                    post_data['click_rate'] = element['stats']['email']['click_rate']
                    post_data['opens'] = element['stats']['email']['opens']
                    post_data['unique_opens'] = element['stats']['email']['unique_opens']
                    post_data['open_rate'] = element['stats']['email']['open_rate']
                    post_data['unsubscribes'] = element['stats']['email']['unsubscribes']
                    post_data['spam_reports'] = element['stats']['email']['spam_reports']
                    post_data['urls'] = {
                        n: {
                            'post_id': element['id'],
                            'publication_id': pub_id,
                            'url': url['url'],
                            'url_clicks': url['total_clicks'],
                            'url_unique_clicks': url['total_unique_clicks'],
                            'url_click_through_rate': url['total_click_through_rate']
                        } for n, url in enumerate(element['stats']['clicks'])
                    }
                    posts_responses.append(post_data)
                else:
                    break_for = True
                    break
            if break_for:
                break
        return posts_responses

    def get_segments_info(pub_id, pub_name):
        segments_responses = []
        for i in range(1, 11):
            segment_params["page"] = i
            segments_temp_response = requests.get(f"https://api.beehiiv.com/v2/publications/{pub_id}/segments", headers=api_headers, params=segment_params)
            segments_temp_response = segments_temp_response.json()
            segments_temp_response['data'] = [element for element in segments_temp_response['data'] if element['status'] == 'completed']
            for element in segments_temp_response['data']:
                segment_data = {}
                segment_data['publication_id'] = pub_id
                segment_data['publication_name'] = pub_name
                segment_data['segment_id'] = element['id']
                segment_data['segment_name'] = element['name']
                segment_data['segment_type'] = 'Dinámico' if element['type'] == 'dynamic' else 'Estático' if element['type'] == 'static' else 'Manual'
                segment_data['last_calculated'] = datetime.fromtimestamp(element['last_calculated']).strftime('%Y-%m-%d')
                segment_data['total_results'] = element['total_results']
                segment_data['status'] = 'Completado'
                segments_responses.append(segment_data)
        return segments_responses

    def add_pubs_info(names, top_date):
        for key, value in names.items():
            posts_responses = get_posts_info(value['id'], top_date, key)
            segments_responses = get_segments_info(value['id'], key)
            if posts_responses:
                names[key]['publication_posts'] = posts_responses
            if segments_responses:
                names[key]['publication_segments'] = segments_responses
        return names

    top_date = datetime.now().replace(microsecond=0) - timedelta(days=7)
    names = get_pubs_info()
    fixed_data = add_pubs_info(names, top_date)
    return fixed_data

def create_db_rows(beehiiv_info, facebook_info):
    rows = {
        'newsletter_performance_table': {
            'columns': "(post_id, publication_id, publication_name, publish_date, delivered, clicks, unique_clicks, click_rate, opens, unique_opens, open_rate, unsubscribes, spam_reports)",
            'rows': []
        },
        'url_performance_table': {
            'columns': "(post_id, publication_id, url, url_clicks, url_unique_clicks, url_click_through_rate)",
            'rows': []
        },
        'unified_performance_table': {
            'columns': "(post_id, publication_id, publication_name, publish_date, delivered, clicks, unique_clicks, click_rate, opens, unique_opens, open_rate, unsubscribes, spam_reports, url, url_clicks, url_unique_clicks, url_click_through_rate)",
            'rows': []
        },
        'publications_table': {
            'columns': "(publication_id, publication_name, organization_name, active_subscriptions, active_premium_subscriptions, active_free_subscriptions, average_open_rate, average_click_rate, total_sent, total_unique_opened, total_clicked)",
            'rows': []
        },
        'segments_table': {
            'columns': "(publication_id, publication_name, segment_id, segment_name, segment_type, last_calculated, total_results, segment_status)",
            'rows': []
        },
        'ad_account_table': {
            'columns': "(account_id, name, status, currency, spend, clicks, unique_clicks, impressions, reach, cost_per_click, click_through_rate, objective, created_time, updated_time)",
            'rows': []
        },
        'campaign_table': {
            'columns': "(campaign_id, ad_account_id, name, status, objective, budget, spend, clicks, unique_clicks, impressions, reach, cost_per_click, click_through_rate, created_time, start_time, stop_time, updated_time)",
            'rows': []
        },
        'ad_set_audience_table': {
            'columns': "(ad_set_id, campaign_id, name, status, objective, bid_amount, bid_strategy, billing_event, budget_remaining, age_targeting, geo_targeting, age, gender, spend, clicks, unique_clicks, impressions, reach, cost_per_click, click_through_rate, created_time, start_time, stop_time, updated_time)",
            'rows': []
        },
        'ad_set_location_table': {
            'columns': "(ad_set_id, campaign_id, name, status, objective, bid_amount, bid_strategy, billing_event, budget_remaining, age_targeting, geo_targeting, region, country, spend, clicks, unique_clicks, impressions, reach, cost_per_click, click_through_rate, created_time, start_time, stop_time, updated_time)",
            'rows': []
        },
        'ad_audience_table': {
            'columns': "(ad_id, ad_set_id, name, status, age, gender, spend, clicks, unique_clicks, impressions, reach, cost_per_click, click_through_rate, created_time, start_time, stop_time, updated_time)",
            'rows': []
        },
        'ad_location_table': {
            'columns': "(ad_id, ad_set_id, name, status, region, country, spend, clicks, unique_clicks, impressions, reach, cost_per_click, click_through_rate, created_time, start_time, stop_time, updated_time)",
            'rows': []
        }
    }

    for nls in beehiiv_info.values():
        rows['publications_table']['rows'].append((
            nls['id'], 
            nls['name'], 
            nls['organization_name'],
            nls['publication_stats']['active_subscriptions'], 
            nls['publication_stats']['active_premium_subscriptions'],
            nls['publication_stats']['active_free_subscriptions'], 
            nls['publication_stats']['average_open_rate'],
            nls['publication_stats']['average_click_rate'], 
            nls['publication_stats']['total_sent'],
            nls['publication_stats']['total_unique_opened'], 
            nls['publication_stats']['total_clicked']
        ))

        if 'publication_segments' in nls:
            for segment in nls['publication_segments']:
                rows['segments_table']['rows'].append((
                    segment['publication_id'],
                    segment['publication_name'],
                    segment['segment_id'],
                    segment['segment_name'],
                    segment['segment_type'],
                    segment['last_calculated'],
                    segment['total_results'],
                    segment['status']
                ))

        if 'publication_posts' in nls:
            for post in nls['publication_posts']:
                rows['newsletter_performance_table']['rows'].append((
                    post['post_id'],
                    post['publication_id'],
                    post['publication_name'],
                    post['publish_date'],
                    post['delivered'],
                    post['clicks'],
                    post['unique_clicks'],
                    post['click_rate'],
                    post['opens'],
                    post['unique_opens'],
                    post['open_rate'],
                    post['unsubscribes'],
                    post['spam_reports']
                ))

                for url in post['urls'].values():
                    rows['url_performance_table']['rows'].append((
                        url['post_id'],
                        url['publication_id'],
                        url['url'],
                        url['url_clicks'],
                        url['url_unique_clicks'],
                        url['url_click_through_rate']
                    ))

                    rows['unified_performance_table']['rows'].append((
                        post['post_id'],
                        post['publication_id'],
                        post['publication_name'],
                        post['publish_date'],
                        post['delivered'],
                        post['clicks'],
                        post['unique_clicks'],
                        post['click_rate'],
                        post['opens'],
                        post['unique_opens'],
                        post['open_rate'],
                        post['unsubscribes'],
                        post['spam_reports'],
                        url['url'],
                        url['url_clicks'],
                        url['url_unique_clicks'],
                        url['url_click_through_rate']
                    ))

    for ad_account in facebook_info:
        rows['ad_account_table']['rows'].append((
            ad_account['id'],
            ad_account['name'],
            'ACTIVE',
            ad_account['currency'],
            sum(insight['spend'] for insight in ad_account['insights_location']) if 'insights_location' in ad_account else 0,
            sum(insight['clicks'] for insight in ad_account['insights_location']) if 'insights_location' in ad_account else 0,
            sum(insight['unique_clicks'] for insight in ad_account['insights_location']) if 'insights_location' in ad_account else 0,
            sum(insight['impressions'] for insight in ad_account['insights_location']) if 'insights_location' in ad_account else 0,
            sum(insight['reach'] for insight in ad_account['insights_location']) if 'insights_location' in ad_account else 0,
            sum(insight['cpc'] for insight in ad_account['insights_location']) / len(ad_account['insights_location']) if 'insights_location' in ad_account and ad_account['insights_location'] else 0,
            sum(insight['ctr'] for insight in ad_account['insights_location']) / len(ad_account['insights_location']) if 'insights_location' in ad_account and ad_account['insights_location'] else 0,
            None,
            ad_account['created_time'],
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ))

        if 'campaigns' in ad_account:
            for campaign in ad_account['campaigns']:
                rows['campaign_table']['rows'].append((
                    campaign['id'],
                    ad_account['id'],
                    campaign['name'],
                    campaign['status'],
                    campaign['objective'],
                    float(campaign['daily_budget']) if 'daily_budget' in campaign else float(campaign['lifetime_budget']) if 'lifetime_budget' in campaign else 0,
                    sum(insight['spend'] for insight in campaign['insights_location']) if 'insights_location' in campaign else 0,
                    sum(insight['clicks'] for insight in campaign['insights_location']) if 'insights_location' in campaign else 0,
                    sum(insight['unique_clicks'] for insight in campaign['insights_location']) if 'insights_location' in campaign else 0,
                    sum(insight['impressions'] for insight in campaign['insights_location']) if 'insights_location' in campaign else 0,
                    sum(insight['reach'] for insight in campaign['insights_location']) if 'insights_location' in campaign else 0,
                    sum(insight['cpc'] for insight in campaign['insights_location']) / len(campaign['insights_location']) if 'insights_location' in campaign and campaign['insights_location'] else 0,
                    sum(insight['ctr'] for insight in campaign['insights_location']) / len(campaign['insights_location']) if 'insights_location' in campaign and campaign['insights_location'] else 0,
                    campaign['created_time'],
                    campaign['start_time'],
                    campaign['stop_time'],
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                ))

                if 'ad_sets' in campaign:
                    for ad_set in campaign['ad_sets']:
                        if 'insights_audience' in ad_set:
                            for insight in ad_set['insights_audience']:
                                rows['ad_set_audience_table']['rows'].append((
                                    ad_set['id'],
                                    campaign['id'],
                                    ad_set['name'],
                                    ad_set['status'],
                                    campaign['objective'],
                                    float(ad_set['bid_amount']) if 'bid_amount' in ad_set else 0,
                                    ad_set['bid_strategy'] if 'bid_strategy' in ad_set else None,
                                    ad_set['billing_event'] if 'billing_event' in ad_set else None,
                                    float(ad_set['daily_budget']) if 'daily_budget' in ad_set else float(ad_set['lifetime_budget']) if 'lifetime_budget' in ad_set else 0,
                                    ad_set['targeting']['age_min'] if 'targeting' in ad_set and 'age_min' in ad_set['targeting'] else None,
                                    json.dumps(ad_set['targeting']['geo_locations']) if 'targeting' in ad_set and 'geo_locations' in ad_set['targeting'] else None,
                                    insight['age'],
                                    insight['gender'],
                                    insight['spend'],
                                    insight['clicks'],
                                    insight['unique_clicks'],
                                    insight['impressions'],
                                    insight['reach'],
                                    insight['cpc'],
                                    insight['ctr'],
                                    ad_set['created_time'],
                                    ad_set['start_time'],
                                    ad_set['stop_time'],
                                    datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                ))

                        if 'insights_location' in ad_set:
                            for insight in ad_set['insights_location']:
                                rows['ad_set_location_table']['rows'].append((
                                    ad_set['id'],
                                    campaign['id'],
                                    ad_set['name'],
                                    ad_set['status'],
                                    campaign['objective'],
                                    float(ad_set['bid_amount']) if 'bid_amount' in ad_set else 0,
                                    ad_set['bid_strategy'] if 'bid_
                                                                        ad_set['bid_strategy'] if 'bid_strategy' in ad_set else None,
                                    ad_set['billing_event'] if 'billing_event' in ad_set else None,
                                    float(ad_set['daily_budget']) if 'daily_budget' in ad_set else float(ad_set['lifetime_budget']) if 'lifetime_budget' in ad_set else 0,
                                    ad_set['targeting']['age_min'] if 'targeting' in ad_set and 'age_min' in ad_set['targeting'] else None,
                                    json.dumps(ad_set['targeting']['geo_locations']) if 'targeting' in ad_set and 'geo_locations' in ad_set['targeting'] else None,
                                    insight['region'],
                                    insight['country'],
                                    insight['spend'],
                                    insight['clicks'],
                                    insight['unique_clicks'],
                                    insight['impressions'],
                                    insight['reach'],
                                    insight['cpc'],
                                    insight['ctr'],
                                    ad_set['created_time'],
                                    ad_set['start_time'],
                                    ad_set['stop_time'],
                                    datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                ))

                        if 'ads' in ad_set:
                            for ad in ad_set['ads']:
                                if 'insights_audience' in ad:
                                    for insight in ad['insights_audience']:
                                        rows['ad_audience_table']['rows'].append((
                                            ad['id'],
                                            ad_set['id'],
                                            ad['name'],
                                            ad['status'],
                                            insight['age'],
                                            insight['gender'],
                                            insight['spend'],
                                            insight['clicks'],
                                            insight['unique_clicks'],
                                            insight['impressions'],
                                            insight['reach'],
                                            insight['cpc'],
                                            insight['ctr'],
                                            ad['created_time'],
                                            None,
                                            None,
                                            datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                        ))

                                if 'insights_location' in ad:
                                    for insight in ad['insights_location']:
                                        rows['ad_location_table']['rows'].append((
                                            ad['id'],
                                            ad_set['id'],
                                            ad['name'],
                                            ad['status'],
                                            insight['region'],
                                            insight['country'],
                                            insight['spend'],
                                            insight['clicks'],
                                            insight['unique_clicks'],
                                            insight['impressions'],
                                            insight['reach'],
                                            insight['cpc'],
                                            insight['ctr'],
                                            ad['created_time'],
                                            None,
                                            None,
                                            datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                        ))

    return rows

def insert_db_data(connection, cursor, rows):
    try:
        for table_name, table_data in rows.items():
            # Limpiar tabla existente
            cursor.execute(f"TRUNCATE TABLE {table_name} CASCADE;")
            
            # Insertar nuevos datos
            if table_data['rows']:
                args_str = ','.join(cursor.mogrify("(" + ",".join(["%s"] * len(row)) + ")", row).decode('utf-8') 
                                  for row in table_data['rows'])
                cursor.execute(f"""
                    INSERT INTO {table_name} {table_data['columns']}
                    VALUES {args_str}
                """)
        
        connection.commit()
        logging.info("Datos insertados exitosamente")
        
    except Exception as e:
        connection.rollback()
        logging.error(f"Error insertando datos: {str(e)}")
        raise
    finally:
        cursor.close()
        connection.close()