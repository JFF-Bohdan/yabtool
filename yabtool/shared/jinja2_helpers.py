import os

from jinja2 import BaseLoader, Environment, StrictUndefined


def jinja2_custom_filter_extract_year_four_digits(value):
    return value.strftime("%Y")


def jinja2_custom_filter_extract_month_two_digits(value):
    return value.strftime("%m")


def jinja2_custom_filter_extract_day_two_digits(value):
    return value.strftime("%d")


def create_rendering_environment():

    env = Environment(loader=BaseLoader, undefined=StrictUndefined)

    env.filters["extract_year_four_digits"] = jinja2_custom_filter_extract_year_four_digits
    env.filters["extract_month_two_digits"] = jinja2_custom_filter_extract_month_two_digits
    env.filters["extract_day_two_digits"] = jinja2_custom_filter_extract_day_two_digits
    env.filters["base_name"] = os.path.basename

    return env
