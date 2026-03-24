{% macro date_spine(start_date, end_date) %}

    /*
        Generates a continuous series of dates between start_date and end_date.
        Useful for filling gaps in time series data.

        Usage:
            {{ date_spine("'2020-01-01'", "current_date()") }}
    */

    select
        date_add(
            cast({{ start_date }} as date),
            interval off day
        ) as date_day
    from
        unnest(
            generate_array(
                0,
                date_diff(
                    cast({{ end_date }} as date),
                    cast({{ start_date }} as date),
                    day
                )
            )
        ) as off

{% endmacro %}
