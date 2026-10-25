import requests
import json
import re
import zipfile
import os
import pickle
from itertools import product
from json import JSONDecodeError
from concurrent.futures import ProcessPoolExecutor
from collections import defaultdict

os.makedirs("queries", exist_ok=True)
os.makedirs("scan_tasks", exist_ok=True)

def save_query(query: str):
    """Query kielipankki.fi and save the results to a json file: queries/[query].json"""
    q_json = f"queries/{query}.json"

    # check if file exists
    if os.path.exists(q_json):
        print(f"Query results for {query} found: {q_json}")
        return

    print(f"Querying kielipankki.fi: '{query}'")

    max_items = 999999  # max items in the json returned by query

    ### case sensitive:
    # query_url = f"https://www.kielipankki.fi/korp/api8/query?default_context=1%20sentence&show=sentence%2Cparagraph%2Clemma%2Clemmacomp%2Cpos%2Cmsd%2Cdephead%2Cdeprel%2Cref%2Clex%2Cspaces%2Cne_name%2Cne_ex%2Cne_type%2Cne_subtype%2Cne_fulltype%2Cne_placename%2Cne_placename_source%2Cne1_name%2Cne1_ex%2Cne1_type%2Cne1_subtype%2Cne1_fulltype%2Cne1_placename%2Cne1_placename_source%2Cne2_name%2Cne2_ex%2Cne2_type%2Cne2_subtype%2Cne2_fulltype%2Cne2_placename%2Cne2_placename_source%2Cnertag2%2Cnertags2%2Cnerbio2&show_struct=text_title%2Ctext_title_orig%2Ctext_date%2Ctext_time%2Ctext_author%2Ctext_author_orig%2Ctext_author_logged_in%2Ctext_author_nick_registered%2Ctext_topic_names%2Ctext_topic_names_orig%2Ctext_topic_name_top%2Ctext_topic_name_leaf%2Ctext_topic_names_set%2Ctext_topic_nums%2Ctext_topic_nums_set%2Ctext_topic_adultonly%2Ctext_msg_type%2Ctext_empty%2Ctext_id%2Ctext_msg_id%2Ctext_thread_id%2Ctext_thread_start_datetime%2Ctext_comment_id%2Ctext_parent_comment_id%2Ctext_parent_datetime%2Ctext_quoted_comment_id%2Ctext_filename_vrt%2Cparagraph_type%2Csentence_id%2Csentence_sentiment_polarity%2Csentence_polarity%2Ctext_sum_lang%2Cparagraph_sum_lang%2Csentence_lang%2Csentence_lang_conf%2Ctext_thread_closed&start=0&end={max_items}&corpus=S24_2001%2CS24_2002%2CS24_2003%2CS24_2004%2CS24_2005%2CS24_2006%2CS24_2007%2CS24_2008%2CS24_2009%2CS24_2010%2CS24_2011%2CS24_2012%2CS24_2013%2CS24_2014%2CS24_2015%2CS24_2016%2CS24_2017%2CS24_2018%2CS24_2019%2CS24_2020%2CS24_2021%2CS24_2022%2CS24_2023&cqp=%5Bword%20%3D%20%22.*{query}.*%22%5D&query_data=&context=&incremental=true&default_within=sentence&within="

    ### case insensitive 2001 - 2023:
    # query_url = f"https://www.kielipankki.fi/korp/api8/query?default_context=1%20sentence&show=sentence%2Cparagraph%2Clemma%2Clemmacomp%2Cpos%2Cmsd%2Cdephead%2Cdeprel%2Cref%2Clex%2Cspaces%2Cne_name%2Cne_ex%2Cne_type%2Cne_subtype%2Cne_fulltype%2Cne_placename%2Cne_placename_source%2Cne1_name%2Cne1_ex%2Cne1_type%2Cne1_subtype%2Cne1_fulltype%2Cne1_placename%2Cne1_placename_source%2Cne2_name%2Cne2_ex%2Cne2_type%2Cne2_subtype%2Cne2_fulltype%2Cne2_placename%2Cne2_placename_source%2Cnertag2%2Cnertags2%2Cnerbio2&show_struct=text_title%2Ctext_title_orig%2Ctext_date%2Ctext_time%2Ctext_author%2Ctext_author_orig%2Ctext_author_logged_in%2Ctext_author_nick_registered%2Ctext_topic_names%2Ctext_topic_names_orig%2Ctext_topic_name_top%2Ctext_topic_name_leaf%2Ctext_topic_names_set%2Ctext_topic_nums%2Ctext_topic_nums_set%2Ctext_topic_adultonly%2Ctext_msg_type%2Ctext_empty%2Ctext_id%2Ctext_msg_id%2Ctext_thread_id%2Ctext_thread_start_datetime%2Ctext_comment_id%2Ctext_parent_comment_id%2Ctext_parent_datetime%2Ctext_quoted_comment_id%2Ctext_filename_vrt%2Cparagraph_type%2Csentence_id%2Csentence_sentiment_polarity%2Csentence_polarity%2Ctext_sum_lang%2Cparagraph_sum_lang%2Csentence_lang%2Csentence_lang_conf%2Ctext_thread_closed&start=0&end={max_items}&corpus=S24_2001%2CS24_2002%2CS24_2003%2CS24_2004%2CS24_2005%2CS24_2006%2CS24_2007%2CS24_2008%2CS24_2009%2CS24_2010%2CS24_2011%2CS24_2012%2CS24_2013%2CS24_2014%2CS24_2015%2CS24_2016%2CS24_2017%2CS24_2018%2CS24_2019%2CS24_2020%2CS24_2021%2CS24_2022%2CS24_2023&cqp=%5Bword%20%3D%20%22.*{query}.*%22%20%25c%5D&query_data=&context=&incremental=true&default_within=sentence&within="

    ### case insensitive 2001 - 2017:
    query_url = f"https://www.kielipankki.fi/korp/api8/query?default_context=1%20sentence&show=sentence%2Cparagraph%2Clemma%2Clemmacomp%2Cpos%2Cmsd%2Cdephead%2Cdeprel%2Cref%2Clex%2Cspaces%2Cne_name%2Cne_ex%2Cne_type%2Cne_subtype%2Cne_fulltype%2Cne_placename%2Cne_placename_source%2Cne1_name%2Cne1_ex%2Cne1_type%2Cne1_subtype%2Cne1_fulltype%2Cne1_placename%2Cne1_placename_source%2Cne2_name%2Cne2_ex%2Cne2_type%2Cne2_subtype%2Cne2_fulltype%2Cne2_placename%2Cne2_placename_source%2Cnertag2%2Cnertags2%2Cnerbio2&show_struct=text_title%2Ctext_title_orig%2Ctext_date%2Ctext_time%2Ctext_author%2Ctext_author_orig%2Ctext_author_logged_in%2Ctext_author_nick_registered%2Ctext_topic_names%2Ctext_topic_names_orig%2Ctext_topic_name_top%2Ctext_topic_name_leaf%2Ctext_topic_names_set%2Ctext_topic_nums%2Ctext_topic_nums_set%2Ctext_topic_adultonly%2Ctext_msg_type%2Ctext_empty%2Ctext_id%2Ctext_msg_id%2Ctext_thread_id%2Ctext_thread_start_datetime%2Ctext_comment_id%2Ctext_parent_comment_id%2Ctext_parent_datetime%2Ctext_quoted_comment_id%2Ctext_filename_vrt%2Cparagraph_type%2Csentence_id%2Csentence_sentiment_polarity%2Csentence_polarity%2Ctext_sum_lang%2Cparagraph_sum_lang%2Csentence_lang%2Csentence_lang_conf&start=0&end={max_items}&corpus=S24_2001%2CS24_2002%2CS24_2003%2CS24_2004%2CS24_2005%2CS24_2006%2CS24_2007%2CS24_2008%2CS24_2009%2CS24_2010%2CS24_2011%2CS24_2012%2CS24_2013%2CS24_2014%2CS24_2015%2CS24_2016%2CS24_2017&cqp=%5Bword%20%3D%20%22.*{query}.*%22%20%25c%5D&query_data=&context=&incremental=true&default_within=sentence&within="

    r = requests.get(query_url)
    data = r.json()

    # save results in a json file
    # try:
    with open(q_json, "w") as f:
        json.dump(data, f)
    print(f"Saved query results to: {q_json}")
    # except:
    #     print(f"Failed to save query results to file {q_json}")


def get_query(query: str):
    """Call save_query() first. Read query results from disk and return as json"""

    q_json = f"queries/{query}.json"

    try:
        with open(q_json, "r") as f:
            data = json.load(f)
            # print(f"Query results for '{query}' found in file: {q_json}")
        return data
    except FileNotFoundError:
        print(f"File {q_json} not found.")
        return None
    except JSONDecodeError:
        print(f"JSONDecodeError for file {q_json}.")
        return None

def get_query_hits(query: str) -> set[tuple]:
    """
    :param query:
    # :return: set() = {(corpus, thread_id, query), ...}   a set of thread_ids found in query
    """

    # threads = defaultdict(int)
    threads = set()

    # load json
    data = get_query((query))

    kwic = data["kwic"]

    for comment in kwic:
        thread_id = comment["structs"]["text_thread_id"].encode("utf-8")  # save as bytes for faster parsing later
        corpus = comment["corpus"]
        threads.add((corpus, thread_id, query))  # include query because multiple queries might be in the same thread

    return threads


def get_thread_ids(query: str) -> set[bytes]:
    """
    :param query:
    # :return: set() = {thread_id, ...}   a set of thread_ids found in query
    """
    thread_ids = set()

    # load json
    data = get_query((query))

    kwic = data["kwic"]
    for comment in kwic:
        thread_id = comment["structs"]["text_thread_id"].encode("utf-8")  # save as bytes for faster parsing later
        thread_ids.add(thread_id)

    return thread_ids


"""
vocabularies = {
    query: {  # query as string
        corpus_name: {  # path as string
            thread_id: {    # byte
                lemma: count    # byte: int
            }
        }
    }
}
"""
vocabularies = defaultdict(  # see dict structure above
    lambda: defaultdict(
        lambda: defaultdict(
            lambda: defaultdict(int)
        )
    )
)


def scan_corpus(corpus_path: str, thread_ids: set[bytes], query: str, zf: zipfile.ZipFile):
    regex_thread_id = re.compile(rb'thread_id="([^"]+)"')
    text_open = b'<text'
    text_close = b'</text>'
    info_open = b'<'

    # corpus_dicts = []
    corpus_vocab = defaultdict(lambda: defaultdict(int))

    with zf.open(f'{corpus_path}') as f:

        # unsaved_text = deque()
        inside_text = False

        for line in f:
            # check if start of text
            if line.startswith(text_open):
                match = regex_thread_id.search(line)
                if match:
                    thread_id = match.group(1)
                else:
                    thread_id = None

                # check if relevant thread
                inside_text = thread_id in thread_ids
                continue

            if not inside_text:
                continue

            if line.startswith(text_close):
                inside_text = False
                continue

            if line.startswith(info_open):
                continue

                # add lemma to dict
            fields = line.rstrip().split(b'\t')
            lemma = fields[2]  # don't change lemma capitalization
            corpus_vocab[thread_id][lemma] += 1

    return {  # can't have lambdas in the return
        thread_id: dict(lemma_counts)
        for thread_id, lemma_counts in corpus_vocab.items()
    }

def query(queries: list, archive_path: str):
    with zipfile.ZipFile(archive_path, mode="r") as zf:
        names = zf.namelist()[0:17]  # include 2001-2017, exclude LICENSE.txt and README.txt

    # download all queries
    for q in queries:
        save_query(q)

separator = "_QUERY_"

def scan_task(args):
    query, path, archive_path = args
    thread_ids = get_thread_ids(query)

    with zipfile.ZipFile(archive_path, mode="r") as zf:
        corpus_vocab = scan_corpus(path, thread_ids, query, zf)

    # save results
    with open(f"scan_tasks/{query}{separator}{path.replace('/', '@')}.pkl", "wb") as f:
        pickle.dump(corpus_vocab, f)

    print(f"Scan complete: {query}, {path[-12:-4]}")
    # return (query, path, corpus_vocab)

def scan(queries: list, archive_path: str):
    # set up the queue for each query term+corpus combination
    with zipfile.ZipFile(archive_path, mode="r") as zf:
        names = zf.namelist()[0:17]  # include 2001-2017, exclude LICENSE.txt and README.txt

    existing = set(os.listdir("scan_tasks"))

    scan_list = [
        (query, path, archive_path)
        for query, path in product(queries, names)
        if f"{query}{separator}{path.replace('/', '@')}.pkl"
           not in existing
    ]

    print(f"Running scan {len(scan_list)} times. This will take around (6-10 min X {len(scan_list)}) / core_count on an SSD. To rerun the same task, delete the .pkl files in scan_tasks/.")
    # print(scan_list)
    total = len(scan_list)

    with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        list(executor.map(scan_task, scan_list))
        # for query, path, corpus_vocab in executor.map(scan_task, scan_list):
        #     pass
        # vocabularies[query][path] = corpus_vocab
        # print(f"Completed scan {i}/{total}: {query} {path[-12:-4]}")

    for file_name in os.listdir("scan_tasks"):
        q, p = file_name.split(separator, 1)
        p = p.removesuffix(".pkl").replace('@', '/')

        with open(f"scan_tasks/{file_name}", "rb") as f:
            corpus_vocab = pickle.load(f)

        vocabularies[q][p] = corpus_vocab

    print("Scan complete.")
    return vocabularies

if __name__ == "__main__":
    print("archive_reader.py main()")
    # defining query terms
    queries = ["deradik", "maltillistu", "polaris", "radik"]
    # don't use "deradik" for analysis
    archive = "suomi24-2001-2017-vrt-v1-3.zip"

    query(queries, archive)
    scan(queries, archive)