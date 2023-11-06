from pathlib import Path
from setuptools import setup

from rework import __version__


doc = Path(__file__).parent / 'README.md'


setup(name='rework',
      version=__version__,
      author='Aurelien Campeas',
      author_email='aurelien.campeas@pythonian.fr',
      description='A database backed distributed task dispatcher',
      long_description=doc.read_text(),
      long_description_content_type='text/markdown',
      url='https://hg.sr.ht/~pythonian/rework',
      packages=['rework'],
      zip_safe=False,
      install_requires=[
          'psutil',
          'colorama',
          'sqlalchemy < 2',
          'sqlhelp',
          'psycopg2-binary',
          'click',
          'tzlocal',
          'inireader',
          'python-icron',
          'pystuck',
          'pyzstd',
          'python-dateutil',
          'psyl',
          'pytz'
      ],
      package_data={'rework': [
          'schema.sql'
      ]},
      extras_require={
          'test': [
              'pytest',
              'pytest_sa_pg',
          ],
          'debug': [
              'pystuck'
          ]
      },
      entry_points={
          'console_scripts': [
              'rework=rework.cli:rework'
          ]
      },
      classifiers=[
          'Development Status :: 4 - Beta',
          'Intended Audience :: Developers',
          'License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)',
          'Operating System :: OS Independent',
          'Programming Language :: Python :: 3',
          'Topic :: Database',
          'Topic :: System :: Distributed Computing',
          'Topic :: Software Development :: Object Brokering'
      ]
)
