
## Requirements

This notebook was written for Python 2.7, and requires the following packages:
- `lxml==4.0.0`
- `elasticsearch==5.4.0`
- `xmltodict==0.11.0h`

## Environment

Define environment variables:
- `DATASET_DIR`:
Path to the dataset directory.

- `ES_INDEX_NAME`:
The elasticsearch index where you want the documents to be uploaded.

- `ES_DOCUMENT_TYPE`:
The document type you want to use for each uploaded document.

- `ES_HOST`:
The connection string for accessing Elasticsearch instance. The format is as follows:
```
http://<username>:<password>@<host>:<port>
```
Example: `http://user:pass@localhost:9200`. If the ES instance doesn't require authentication, you can specify `http://<host>:<port>` as the connection string. If `ES_HOST = ''`, `http://localhost:9200` will be used as the connection string.
- `ES_CREATE_INDEX`:
Create index if necessary.


```python
DATASET_DIR      = 'receipt-id-430111-jcodes-klmnop-part-001/'
ES_INDEX_NAME    = 'jstor'
ES_DOCUMENT_TYPE = 'article'
ES_HOST          = 'http://username:password@localhost:9200'
ES_CREATE_INDEX  = True
```

### Logging

To get realtime logs of processed directories, set ``'dataset'`` logger's level to `logging.INFO` or `logging.DEBUG`.  
  
  
**Note**: `'elasticsearch'` logger generates high amounts of debug log statements.


```python
import logging
logging.basicConfig(level=logging.CRITICAL, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.WARNING)

## Set to INFO/DEBUG to get realtime logs of processed directories.
# logger.setLevel(logging.INFO)
```

## Supporting Implementation

### UTF8 encoding

*NOTE: This step is not needed for Python 3 and above.*  

Ensure all strings use `utf-8` encoding by default, else you may run into `ordinal not in range` errors.


```python
import sys
reload(sys)
sys.setdefaultencoding('UTF8')
```

### Parsers


```python
import xml.etree.ElementTree as ET
import xmltodict

from lxml import etree

class XMLParser(object):
    def _process_if_avail(self, d, key, func):
        try:
            value = d.pop(key)
        except KeyError:
            return
        else:
            if value is not None:
                new_value = func(value)
                if new_value is not None:
                    d[key] = new_value

    def _process__article_meta__contrib_group(self, contrib_groups):
        if not isinstance(contrib_groups, list):
            contrib_groups = [contrib_groups]

        for contrib_group in contrib_groups:
            if not isinstance(contrib_group['contrib'], list):
                contrib_group['contrib'] = [contrib_group['contrib']]

            try:
                if isinstance(contrib_group['aff'], basestring):
                    contrib_group['aff'] = [contrib_group['aff']]
            except KeyError:
                pass

            for i, contrib in enumerate(contrib_group['contrib']):
                try:
                    name = contrib['string-name']
                except KeyError:
                    pass
                else:
                    if isinstance(name, dict):
                        name = u'{} {}'.format(name.get('given-names', ''), name.get('surname', ''))
                    contrib_group['contrib'][i]['string-name'] = name
                try:
                    if isinstance(contrib['aff'], basestring):
                        contrib['aff'] = [contrib['aff']]
                except KeyError:
                    pass

        return contrib_groups

    def _process__article_meta__pub_date(self, pub_dates):
        if isinstance(pub_dates, dict):
            if isinstance(pub_dates['year'], list):
                pub_date_list = []
                for i in xrange(len(pub_dates['year'])):
                    date = { }
                    if 'day' in pub_dates.keys(): date['day'] = pub_dates['day'][i]
                    if 'month' in pub_dates.keys(): date['month'] = pub_dates['month'][i]
                    if 'year' in pub_dates.keys(): date['year'] = pub_dates['year'][i]
                    pub_date_list.append(date)

                pub_dates = pub_date_list
            else:
                pub_dates = [pub_dates]

        for pub_date in pub_dates:
            self._process_if_avail(pub_date, 'day',   int)
            self._process_if_avail(pub_date, 'month', int)
            self._process_if_avail(pub_date, 'year',  int)

        return pub_dates

    def _process__article_meta(self, article_meta):
        _make_str = lambda d: d.get('#text', None) if isinstance(d, dict) else d

        # Remove duplicates
        for key in ('issue-id', 'issue', 'volume', 'pub-date'):
            if isinstance(article_meta.get(key, ''), list):
                if article_meta[key][0] == article_meta[key][1]:
                    article_meta[key] = article_meta[key][0]

        self._process_if_avail(article_meta, 'contrib-group', self._process__article_meta__contrib_group)
        self._process_if_avail(article_meta, 'pub-date', self._process__article_meta__pub_date)
        self._process_if_avail(article_meta, 'issue-id', _make_str)
        self._process_if_avail(article_meta, 'issue', _make_str)
        self._process_if_avail(article_meta, 'volume', _make_str)
        try:
            self._process_if_avail(article_meta['title-group'], 'article-title', _make_str)
        except KeyError:
            pass
        return article_meta

    def _process__journal_meta(self, journal_meta):
        journal_title_str = lambda journal_title: journal_title['#text'] if isinstance(journal_title, dict) else journal_title
        self._process_if_avail(journal_meta['journal-title-group'], 'journal-title', journal_title_str)
        return journal_meta

    def _get_parse_postprocessor(self, article_et):
        etree_str = lambda e: etree.tostring(e, encoding='utf-8', method='text').strip()
        self._seen1 = False
        self._seen2 = False
        def postprocessor(path, key, value):
            xpath = '/'.join((path[i][0] for i in xrange(1, len(path))))

            if value is None:
                return None, None

            if key in set((
                '@xmlns:xsi',
                '@xlink:type',
                '@ext-link-type',
                '@content-type',
                '@dtd-version',
                '@xmlns:oasis',
                '@xmlns:xlink',
                '@xmlns:mml',
                '@xlink:role',
                '@xlink:title',
                )):
                return None, None

            if key == 'email':
                if isinstance(value, dict):
                    value = value['#text']
                return key, value

            if key in set((
                'page-count',
                'ref-count',
                'fig-count',
                'equation-count',
                'table-count',
                )):
                return key, int(value['@count'])

            if xpath in set((
                'front/article-meta/kwd-group/x',
                'front/article-meta/contrib-group/x',
                'front/article-meta/contrib-group/xref',
                'front/article-meta/contrib-group/contrib/x',
                'front/article-meta/contrib-group/contrib/xref',
                )):
                return None, None

            if not isinstance(value, basestring):
                if xpath in set((
                    'front/article-meta/abstract',
                    'front/article-meta/trans-abstract',
                    'front/article-meta/title-group/article-title',
                    'front/article-meta/title-group/subtitle',
                    'front/article-meta/title-group/trans-title-group/trans-title',
                    'front/article-meta/author-notes',
                    'front/article-meta/bio',
                    'back/ack',
                    'body',
                    )):
                    elements = article_et.xpath(xpath)
                    value = etree_str(elements[0]).strip()
                    return key, value

                elif xpath in set((
                    'back/app-group/app',
                    'back/fn-group',
                    'back/sec',
                    'front/notes',
                    'front/article-meta/kwd-group/kwd',
                    'front/article-meta/contrib-group/bio',
                    'front/article-meta/contrib-group/fn',
                    )):
                    element = article_et.xpath(xpath + '[count(*)>0]')[0]
                    value = etree_str(element)
                    element.getparent().remove(element)
                    return key, value

                elif xpath == 'back/ref-list/ref/mixed-citation':
                    refid = path[3][1]['id']
                    value = ' '.join(article_et.xpath("back/ref-list/ref[@id='{}']/mixed-citation/text()".format(refid))).strip()
                    return key, value

                elif isinstance(value, dict) and xpath in set((
                    'front/article-meta/contrib-group/aff',
                    'front/article-meta/contrib-group/contrib/aff',
                    )):
                    try:
                        aff = article_et.xpath(xpath + '[count(*)>0]')[0]
                        value = etree_str(aff)
                        aff.getparent().remove(aff)
                    except IndexError:
                        value = value['#text']
                    return key, value

            return key, value

        return postprocessor

    def parse(self, xml_path):
        parser = etree.XMLParser(recover=True)
        article_et = etree.parse(xml_path, parser=parser).getroot()

        with open(xml_path, 'r') as fh:
            article = xmltodict.parse(fh.read(), postprocessor=self._get_parse_postprocessor(article_et))['article']

        article['front']['journal-meta'] = self._process__journal_meta(article['front']['journal-meta'])
        article['front']['article-meta'] = self._process__article_meta(article['front']['article-meta'])

        self._process_if_avail(article['front'], 'notes', lambda v: v if isinstance(v, list) else [v])
        self._process_if_avail(article.get('back', {}), 'sec', lambda v: v if isinstance(v, list) else [v])

        return article

class TXTParser(object):
    def parse(self, txt_path):
        txt_root = ET.parse(txt_path).getroot()
        if txt_root.tag == 'plain_text':
            page_seq = [(p.attrib['sequence'], p.text) for p in list(txt_root)]
            page_seq.sort(key=lambda x: x[0])
            plain_text_pages = [p for s, p in page_seq]
            return {'plain_text': plain_text_pages}
        elif txt_root.tag == 'body':
            return {'body': ET.tostring(txt_root, encoding='utf-8', method='text')}
```

### Dataset article actions generator


```python
import os

def generate_actions(dataset_dir, index, document_type):
    logger = logging.getLogger()
    abs_dataset_dir = os.path.abspath(os.path.expanduser(dataset_dir))
    xmlparser = XMLParser()
    txtparser = TXTParser()
    doc_id = 0
    for (dpath, dnames, fnames) in os.walk(abs_dataset_dir, topdown=False):
        if not fnames:
            continue

        logger.info('Processing %s' % dpath)
        document = {}

        xml_files = [p for p in fnames if p.lower().endswith('.xml')]
        if xml_files:
            if len(xml_files) > 1:
                logger.warning('Multiple xml files found in %s. Ignoring...' % dpath)
            else:
                document['article'] = xmlparser.parse(os.path.join(dpath, xml_files[0]))

        txt_files = [p for p in fnames if p.lower().endswith('.txt')]
        if txt_files:
            if len(txt_files) > 1:
                logger.warning('Multiple txt files found in %s. Ignoring...' % dpath)
            else:
                txt_path = os.path.join(dpath, txt_files[0])
                try:
                    document.update(txtparser.parse(txt_path))
                except ET.ParseError:
                    logger.warning('ERROR reading \'%s\'. Ignoring txt file...' % (txt_path))

        if document:
            action = {
                '_index': index,
                '_type': document_type,
                '_id': doc_id,
                '_source': document
            }
            doc_id += 1
            yield action
```

## Upload to ES

#### Imports


```python
import elasticsearch
import elasticsearch.helpers
```

#### Get client


```python
es = elasticsearch.Elasticsearch([ES_HOST]) if ES_HOST else elasticsearch.Elasticsearch()
```

#### Create Index

Create index if necessary.


```python
# es.indices.delete(index=ES_INDEX_NAME)
if ES_CREATE_INDEX:
    es.indices.create(index=ES_INDEX_NAME, ignore=[400])
```

#### Start Upload

Start (bulk) uploading documents to Elasticsearch. Use `chunk_size` parameter to control how many documents are uploaded in one request. By default, at most `500` documents are uploaded per request.  

Depending on the log level, real-time logs of processed directories may be displayed.


```python
action_generator = generate_actions(DATASET_DIR, index=ES_INDEX_NAME, document_type=ES_DOCUMENT_TYPE)

# elasticsearch.helpers.bulk(es, action_generator, chunk_size=100)
elasticsearch.helpers.bulk(es, action_generator)
```

#### Test

The following command just gets the count of documents.


```python
es.count(index=ES_INDEX_NAME, doc_type=ES_DOCUMENT_TYPE)['count']
```
