from typing import Optional
from pydantic import BaseModel

import polars as pl

class OtherUsers(BaseModel):
    first_name: Optional[str]
    last_name: Optional[str]
    email: Optional[str]
    paid_share: Optional[float]
    owed_share: Optional[float]


expenses_schema_polars = pl.Schema([
    ('cost', pl.String),
    ('description', pl.String),
    ('details', pl.String),
    ('date', pl.String),
    ('repeat_interval', pl.String),
    ('currency_code', pl.String),
    ('category_id', pl.Int64),
    ('id', pl.Int64),
    ('group_id', pl.Int64),
    ('friendship_id', pl.Int64),
    ('expense_bundle_id', pl.Int64),
    ('repeats', pl.Boolean),
    ('email_reminder', pl.Boolean),
    ('email_reminder_in_advance', pl.Int64),
    ('next_repeat', pl.String),
    ('comments_count', pl.Int64),
    ('payment', pl.Boolean),
    ('transaction_confirmed', pl.Boolean),
    (
        'repayments',
        pl.List(
            pl.Struct({
                'from': pl.Int64,
                'to': pl.Int64,
                'amount': pl.String
            })
        )
    ),
    ('created_at', pl.String),
    (
        'created_by',
        pl.Struct({
            'id': pl.Int64,
            'first_name': pl.String,
            'last_name': pl.String,
            'email': pl.String,
            'registration_status': pl.String,
            'picture': pl.Struct({
                'small': pl.String,
                'medium': pl.String,
                'large': pl.String
            }),
            'custom_picture': pl.Boolean
        })
    ),
    ('updated_at', pl.String),
    (
        'updated_by',
        pl.Struct({
            'id': pl.Int64,
            'first_name': pl.String,
            'last_name': pl.String,
            'email': pl.String,
            'registration_status': pl.String,
            'picture': pl.Struct({
                'small': pl.String,
                'medium': pl.String,
                'large': pl.String
            }),
            'custom_picture': pl.Boolean
        })
    ),
    ('deleted_at', pl.String),
    (
        'deleted_by',
        pl.Struct({
            'id': pl.Int64,
            'first_name': pl.String,
            'last_name': pl.String,
            'email': pl.String,
            'registration_status': pl.String,
            'picture': pl.Struct({
                'small': pl.String,
                'medium': pl.String,
                'large': pl.String
            }),
            'custom_picture': pl.Boolean
        })
    ),
    (
        'category',
        pl.Struct({
            'id': pl.Int64,
            'name': pl.String
        })
    ),
    (
        'receipt',
        pl.Struct({
            'large': pl.String,
            'original': pl.String
        })
    ),
    (
        'users',
        pl.List(
            pl.Struct({
                'user': pl.Struct({
                    'id': pl.Int64,
                    'first_name': pl.String,
                    'last_name': pl.String,
                    'picture': pl.Struct({
                        'medium': pl.String
                    })
                }),
                'user_id': pl.Int64,
                'paid_share': pl.String,
                'owed_share': pl.String,
                'net_balance': pl.String
            })
        )
    ),
    (
        'comments',
        pl.List(
            pl.Struct({
                'id': pl.Int64,
                'content': pl.String,
                'comment_type': pl.String,
                'relation_type': pl.String,
                'relation_id': pl.Int64,
                'created_at': pl.String,
                'deleted_at': pl.String,
                'user': pl.Struct({
                    'id': pl.Int64,
                    'first_name': pl.String,
                    'last_name': pl.String,
                    'picture': pl.Struct({
                        'medium': pl.String
                    })
                })
            })
        )
    )
])

users_schema_polars = pl.Schema([
    ('id', pl.Int64),
    ('name', pl.Utf8),
    ('created_at', pl.Utf8),
    ('updated_at', pl.Utf8),
    ('members', pl.List(
        pl.Struct({
            'id': pl.Int64,
            'first_name': pl.Utf8,
            'last_name': pl.Utf8,
            'picture': pl.Struct({
                'small': pl.Utf8,
                'medium': pl.Utf8,
                'large': pl.Utf8
            }),
            'custom_picture': pl.Boolean,
            'email': pl.Utf8,
            'registration_status': pl.Utf8,
            'balance': pl.List(
                pl.Struct({
                    'currency_code': pl.Utf8,
                    'amount': pl.Utf8
                })
            ),
            'default_split': pl.Struct({
                'split_type': pl.Utf8,
                'owed_shares': pl.List(
                    pl.Struct({
                        'user_id': pl.Int64,
                        'owed_share': pl.Utf8
                    })
                )
            })
        })
    )),
    ('simplify_by_default', pl.Boolean),
    ('original_debts', pl.List(
        pl.Struct({
            'to': pl.Int64,
            'from': pl.Int64,
            'amount': pl.Utf8,
            'currency_code': pl.Utf8
        })
    )),
    ('simplified_debts', pl.List(
        pl.Struct({
            'from': pl.Int64,
            'to': pl.Int64,
            'amount': pl.Utf8,
            'currency_code': pl.Utf8
        })
    )),
    ('avatar', pl.Struct({
        'small': pl.Utf8,
        'medium': pl.Utf8,
        'large': pl.Utf8,
        'xlarge': pl.Utf8,
        'xxlarge': pl.Utf8,
        'original': pl.Utf8
    })),
    ('tall_avatar', pl.Struct({
        'xlarge': pl.Utf8,
        'large': pl.Utf8
    })),
    ('custom_avatar', pl.Boolean),
    ('cover_photo', pl.Struct({
        'xxlarge': pl.Utf8,
        'xlarge': pl.Utf8
    })),
    ('whiteboard', pl.String),
    ('group_type', pl.Utf8),
    ('invite_link', pl.Utf8),
    ('group_reminders', pl.String)
])
