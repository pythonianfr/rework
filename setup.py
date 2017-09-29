from setuptools import setup


setup(name='rework',
      version='0.1.0',
      author='Aurelien Campeas',
      author_email='aurelien.campeas@pythonian.fr',
      description='A database-oriented distributed task dispatcher',
      packages=['rework'],
      install_requires=[
          'pytest_sa_pg',
          'pathlib',
          'psutil',
          'colorama',
          'sqlalchemy',
          'psycopg2',
          'click'
      ],
      entry_points={
          'console_scripts': [
              'rework=rework.cli:rework'
          ]
      }
)
