from rework import api


@api.task
def boring_task(task):
    pass


@api.task(domain='scrappers')
def scrap_sites(task):
    pass

