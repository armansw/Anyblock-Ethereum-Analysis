from libs.local import LocalCalc
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from math import log10


def get_main_pie(calc):
    # get the data
    date, lth, sth = calc.get_last_history()

    fig = make_subplots(rows=2, cols=1, specs=[[{'type': 'domain'}], [{'type': 'domain'}]], subplot_titles=('LTH vs STH', 'Log Percentage'))
    pie = go.Pie(values=[lth, sth], labels=['Long-Term Holding', 'Short-Term Holding'])
    log_pie = go.Pie(values=[log10(lth), log10(sth)], labels=['Long-Term Holding', 'Short-Term Holding'])
    fig.add_trace(pie, row=1, col=1)
    fig.add_trace(log_pie, row=2, col=1)
    fig.update_layout(title_text=f'{date:%b %d, %Y}', title_x=0.5)

    return fig


def get_top_address_pie(connection, top_count):
    cursor = connection.cursor()

    # get total balance
    (row, ) = cursor.execute('SELECT SUM(balance) FROM balance').fetchall()
    total_balance = row[0] * 1e-6

    # get top addresses
    rows = cursor.execute('SELECT * FROM balance ORDER BY balance DESC LIMIT {}'.format(top_count)).fetchall()

    # make arrays
    balances = []
    addresses = []
    for rows in rows:
        balance = rows[2] * 1e-6
        balances.append(balance)
        addresses.append(rows[1])

        # subtract top balances
        total_balance = total_balance - balance

    # add Others item
    balances.append(total_balance)
    addresses.append('Others')

    return go.Pie(values=balances, labels=addresses)


def get_history_line(calc):
    data = calc.get_history()

    # stack lines
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=[row[0] for row in data],
        y=[row[1] for row in data],
        fill='tozeroy',
        name='LTH'))
    fig.add_trace(go.Scatter(
        x=[row[0] for row in data],
        y=[row[1] + row[2] for row in data],
        fill='tonexty',
        name='LTH + STH'))

    fig.update_layout(title={
        'text': f'LTH vs STH over time',
        'y': 1.0,
        'x': 0.5,
        'xanchor': 'center',
        'yanchor': 'top'
    })

    return fig


def get_log_history_line(calc):
    data = calc.get_history()

    # stack lines
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=[row[0] for row in data],
        y=[log10(row[1]) for row in data],
        fill='tozeroy',
        name='LTH'))
    fig.add_trace(go.Scatter(
        x=[row[0] for row in data],
        y=[log10(row[1] + row[2]) for row in data],
        fill='tonexty',
        name='LTH + STH'))

    fig.update_layout(title={
        'text': f'Logarithmic LTH vs STH over time',
        'y': 1.0,
        'x': 0.5,
        'xanchor': 'center',
        'yanchor': 'top'
    })

    return fig


def main():
    calculator = LocalCalc()

    get_main_pie(calculator).show()
    get_history_line(calculator).show()
    get_log_history_line(calculator).show()


if __name__ == "__main__":
    main()
