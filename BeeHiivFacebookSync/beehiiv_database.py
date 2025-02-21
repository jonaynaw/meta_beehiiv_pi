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
    
    try:
        business = Business(business_id)
        accounts = business.get_owned_ad_accounts()
        
        data = []
        for account in accounts:
            account_data = {
                'id': account['id'],
                'name': account['name'],
                'campaigns': []
            }
            
            campaigns = account.get_campaigns()
            for campaign in campaigns:
                if campaign['status'] in effective_status:
                    campaign_data = {
                        'id': campaign['id'],
                        'name': campaign['name'],
                        'status': campaign['status'],
                        'insights': campaign.get_insights()
                    }
                    account_data['campaigns'].append(campaign_data)
                    
            data.append(account_data)
            
        return data
        
    except FacebookRequestError as e:
        logging.error(f"Facebook API Error: {str(e)}")
        raise
    except Exception as e:
        logging.error(f"Error fetching Facebook data: {str(e)}")
        raise

def fetch_data_from_beehiiv_api():
    api_headers = {
        "Accept": "application/json",
        "Authorization": os.environ["BEEHIIV_API_KEY"],
    }
    
    try:
        # Endpoint de Beehiiv
        url = "https://api.beehiiv.com/v2/publications"
        response = requests.get(url, headers=api_headers)
        response.raise_for_status()
        
        data = response.json()
        
        # Obtener datos detallados de cada publicación
        detailed_data = []
        for pub in data['data']:
            pub_id = pub['id']
            
            # Obtener métricas
            metrics_url = f"{url}/{pub_id}/metrics"
            metrics_response = requests.get(metrics_url, headers=api_headers)
            metrics_response.raise_for_status()
            
            pub_data = {
                **pub,
                'metrics': metrics_response.json()['data']
            }
            detailed_data.append(pub_data)
            
        return detailed_data
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Beehiiv API Error: {str(e)}")
        raise
    except Exception as e:
        logging.error(f"Error fetching Beehiiv data: {str(e)}")
        raise

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