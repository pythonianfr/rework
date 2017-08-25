import threading
import time


def guard(engine, sql, expr, timeout=6):
    outcome = []

    def loop():
        start = time.time()
        while True:
            if (time.time() - start) > timeout:
                return
            with engine.connect() as cn:
                res = cn.execute(sql)
                got = expr(res)
                if got:
                    outcome.append(got)
                    return
            time.sleep(.1)

    th = threading.Thread(target=loop)
    th.daemon = True
    th.start()
    th.join()
    assert outcome
    return outcome[0]


def scrub(anstr, subst='X'):
    out = []
    digit = False
    for char in anstr:
        if char.isdigit():
            if not digit:
                digit = True
        else:
            if digit:
                digit = False
                out.append('<{}>'.format(subst))
            out.append(char)
    return ''.join(out).strip()
