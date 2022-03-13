from datetime import datetime
from multiprocessing import Condition
from behave import *
import rx
from rx import operators

@given('一個每 {period:d} 秒發出資料共 {elapsed:d} 秒的資料源')
def stepSourceInSecond(ctx, period, elapsed):
    ctx.source = rx.interval(period).pipe(
        operators.map(lambda x: {
            'idx': x,
            'ts': datetime.now()
        }),
        operators.take_with_time(elapsed),
    )

@when('將資料源資料以秒做為鍵值分群')
def stepGroupTicks(ctx):
    ctx.source = ctx.source.pipe(
        operators.group_by_until(
            lambda x: datetime(x['ts'].year,
                x['ts'].month,
                x['ts'].day,
                x['ts'].hour,
                x['ts'].minute,
                x['ts'].second
            ),
            None,
            lambda g: rx.timer(1)
        )
    )

@when('接收資料直到結束')
def stepReceiveUntilCompleted(ctx):
    cv = Condition()
    groupedTicks = {}

    def onGroup(observable: rx.core.GroupedObservable):
        k = observable.key
        g = []
        def onGroupExpired():
            nonlocal groupedTicks, cv
            cv.acquire()
            groupedTicks[k] = g
            cv.release()
        observable.subscribe(
            on_next=lambda x: g.append(x),
            on_completed=onGroupExpired
        )

    def onCompleted():
        cv.acquire()
        cv.notify()
        cv.release()

    ctx.source.subscribe(
        on_next=onGroup,
        on_completed=onCompleted
    )
    cv.acquire()
    cv.wait()
    ctx.groupedTicks = groupedTicks
    cv.release()

@then('每個頻率群組中資料秒數相等於群組鍵值')
def stepVerifyTheSecondShouldEqualToGroupKey(ctx):
    for k in ctx.groupedTicks:
        g = ctx.groupedTicks[k]
        for x in g:
            assert x['ts'].second == k.second, f"[{x['idx']}]{x['ts']} 的秒數應要等於 {k.second}"