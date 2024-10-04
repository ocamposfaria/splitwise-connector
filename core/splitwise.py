import requests
from dotenv import load_dotenv
import os
from typing import Optional, Dict, Any
from icecream import ic

load_dotenv()

class Splitwise:
    def __init__(self):
        self._api_key = os.getenv('API_KEY')
        self._base_url = "https://secure.splitwise.com/api/v3.0"
        self._headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json"
        }
    
    def _make_request(
            self, 
            endpoint: str, 
            method: str = "GET", 
            data: Optional[Dict[str, Any]] = None, 
            params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        
        url = f"{self._base_url}/{endpoint}"
        
        try:
            response = requests.request(
                method,
                url,
                headers=self._headers,
                json=data,
                params=params
            )
            response.raise_for_status()

            json_response = response.json()
            
            return {
                "status_code": response.status_code,
                "message": "Success",
                "data": json_response
            }
        except requests.exceptions.HTTPError as http_err:
            return {
                "status_code": response.status_code,
                "message": f"HTTP error occurred: {http_err}"
            }
        except Exception as err:
            return {
                "status_code": 500,
                "message": f"Other error occurred: {err}"
            }

    def get_expense(self, expense_id):
        return self._make_request(f"get_expense/{expense_id}")
    
    def get_expenses(
        self,
        group_id: Optional[int] = None,
        friend_id: Optional[int] = None,
        dated_after: Optional[str] = None,
        dated_before: Optional[str] = None,
        updated_after: Optional[str] = None,
        updated_before: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None
    ) -> Dict[str, Any]:
        params = {
            "group_id": group_id,
            "friend_id": friend_id,
            "dated_after": dated_after,
            "dated_before": dated_before,
            "updated_after": updated_after,
            "updated_before": updated_before,
            "limit": limit,
            "offset": offset
        }
        
        params = {k: v for k, v in params.items() if v is not None}
        return self._make_request("get_expenses/", params=params)
    
    def get_groups(self):
        return self._make_request(f"get_groups/", method="GET")
    
    def get_current_user(self):
        return self._make_request(f"get_current_user/", method="GET")

    def get_friends(self):
        return self._make_request(f"get_friends/", method="GET")

    def delete_expense(self, expense_id):
        return self._make_request(f"delete_expense/{expense_id}", method="POST")

    def get_group(self, group_id):
        return self._make_request(f"get_group/{group_id}")

    def create_expense(
        self,
        cost,
        description,
        details = None,
        date = None,
        repeat_interval = None,
        currency_code = None,
        category_id = None,
        group_id = None,
        main_user_id = None,
        main_user_paid_share = None,
        main_user_owed_share = None,
        other_users = None
    ):
        data = {}
        
        if cost is not None:
            data['cost'] = "{:.2f}".format(cost)
        if description is not None:
            data['description'] = str(description)
        if details is not None:
            data['details'] = str(details)
        if date is not None:
            data['date'] = str(date)
        if repeat_interval is not None:
            data['repeat_interval'] = str(repeat_interval)
        if currency_code is not None:
            data['currency_code'] = str(currency_code)
        if category_id is not None:
            data['category_id'] = str(category_id)
        if group_id is not None:
            data['group_id'] = group_id
        if main_user_id is not None:
            data["users__0__user_id"] = main_user_id
        if main_user_paid_share is not None:
            data["users__0__paid_share"] = str(main_user_paid_share)
        if main_user_owed_share is not None:
            data["users__0__owed_share"] = str(main_user_owed_share)
        
        if other_users:
            for index, user in enumerate(other_users):
                for key, value in user.dict(exclude_none=True).items():
                    data[f"users__{index+1}__{key}"] = str(value)

        return self._make_request("create_expense/", method="POST", data=data)
    
    def update_expense(
        self,
        expense_id,
        cost = None,
        description = None,
        details = None,
        date = None,
        repeat_interval = None,
        currency_code = None,
        category_id = None,
        group_id = None,
        main_user_id = None,
        main_user_paid_share = None,
        main_user_owed_share = None,
        other_users = None
    ):
        data = {}
        
        if cost is not None:
            data['cost'] = "{:.2f}".format(cost)
        if description is not None:
            data['description'] = str(description)
        if details is not None:
            data['details'] = str(details)
        if date is not None:
            data['date'] = str(date)
        if repeat_interval is not None:
            data['repeat_interval'] = str(repeat_interval)
        if currency_code is not None:
            data['currency_code'] = str(currency_code)
        if category_id is not None:
            data['category_id'] = str(category_id)
        if group_id is not None:
            data['group_id'] = group_id
        if main_user_id is not None:
            data["users__0__user_id"] = main_user_id
        if main_user_paid_share is not None:
            data["users__0__paid_share"] = str(main_user_paid_share)
        if main_user_owed_share is not None:
            data["users__0__owed_share"] = str(main_user_owed_share)
        
        if other_users:
            for index, user in enumerate(other_users):
                for key, value in user.dict(exclude_none=True).items():
                    data[f"users__{index+1}__{key}"] = str(value)

        return self._make_request(f"update_expense/{expense_id}", method="POST", data=data)
