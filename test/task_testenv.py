from rework import api

@api.task(domain='test')
def foo(task):
    pass
