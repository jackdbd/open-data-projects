import plotly
import plotly.express as px

query = f"""
    WITH daily_counts AS (
    SELECT
      date_trunc('day', created_date) AS complaint_date,
      borough,
      COUNT(*) AS daily_complaints
    FROM
      data.main.service_requests
    GROUP BY
      date_trunc('day', created_date),
      borough
    )
    SELECT
      complaint_date,
      daily_complaints,
      borough
    FROM
      daily_counts
    ORDER BY
      complaint_date,
      daily_complaints DESC;
    """

df = conn.execute(query).fetch_df()

fig = px.line(
    df,
    x="complaint_date",
    y="daily_complaints",
    facet_col="borough",
    facet_col_wrap=3,
    title="Daily complaints over time, grouped by NYC borough",
)
# This changes each facet plot title from something like "borough=MANHATTAN" to just "MANHATTAN"
fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))
fig.for_each_xaxis(lambda a: a.update(title="Date"))
fig.for_each_yaxis(lambda a: a.update(title="Complaints reported"))

fig.show()
