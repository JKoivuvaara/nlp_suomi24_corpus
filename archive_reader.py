import random

import pandas as pd
import requests
import json
import re
import zipfile
import os
import pickle
import time
from itertools import product
from json import JSONDecodeError
from concurrent.futures import ProcessPoolExecutor
from collections import defaultdict

os.makedirs("queries", exist_ok=True)
os.makedirs("filtered_queries", exist_ok=True)
os.makedirs("scan_tasks", exist_ok=True)
os.makedirs("comment_samples", exist_ok=True)

def save_query(query: str):
    """Query kielipankki.fi and save the results to a json file: queries/[query].json"""
    q_json = f"queries/{query}.json"

    # check if file exists
    if os.path.exists(q_json):
        print(f"{time.strftime('%H:%M', time.localtime())}: Query results for {query} found: {q_json}")
        return

    print(f"{time.strftime('%H:%M', time.localtime())}: Querying kielipankki.fi: '{query}'")

    max_items = 999999  # max items in the json returned by query

    ### case sensitive:
    # query_url = f"https://www.kielipankki.fi/korp/api8/query?default_context=1%20sentence&show=sentence%2Cparagraph%2Clemma%2Clemmacomp%2Cpos%2Cmsd%2Cdephead%2Cdeprel%2Cref%2Clex%2Cspaces%2Cne_name%2Cne_ex%2Cne_type%2Cne_subtype%2Cne_fulltype%2Cne_placename%2Cne_placename_source%2Cne1_name%2Cne1_ex%2Cne1_type%2Cne1_subtype%2Cne1_fulltype%2Cne1_placename%2Cne1_placename_source%2Cne2_name%2Cne2_ex%2Cne2_type%2Cne2_subtype%2Cne2_fulltype%2Cne2_placename%2Cne2_placename_source%2Cnertag2%2Cnertags2%2Cnerbio2&show_struct=text_title%2Ctext_title_orig%2Ctext_date%2Ctext_time%2Ctext_author%2Ctext_author_orig%2Ctext_author_logged_in%2Ctext_author_nick_registered%2Ctext_topic_names%2Ctext_topic_names_orig%2Ctext_topic_name_top%2Ctext_topic_name_leaf%2Ctext_topic_names_set%2Ctext_topic_nums%2Ctext_topic_nums_set%2Ctext_topic_adultonly%2Ctext_msg_type%2Ctext_empty%2Ctext_id%2Ctext_msg_id%2Ctext_thread_id%2Ctext_thread_start_datetime%2Ctext_comment_id%2Ctext_parent_comment_id%2Ctext_parent_datetime%2Ctext_quoted_comment_id%2Ctext_filename_vrt%2Cparagraph_type%2Csentence_id%2Csentence_sentiment_polarity%2Csentence_polarity%2Ctext_sum_lang%2Cparagraph_sum_lang%2Csentence_lang%2Csentence_lang_conf%2Ctext_thread_closed&start=0&end={max_items}&corpus=S24_2001%2CS24_2002%2CS24_2003%2CS24_2004%2CS24_2005%2CS24_2006%2CS24_2007%2CS24_2008%2CS24_2009%2CS24_2010%2CS24_2011%2CS24_2012%2CS24_2013%2CS24_2014%2CS24_2015%2CS24_2016%2CS24_2017%2CS24_2018%2CS24_2019%2CS24_2020%2CS24_2021%2CS24_2022%2CS24_2023&cqp=%5Bword%20%3D%20%22.*{query}.*%22%5D&query_data=&context=&incremental=true&default_within=sentence&within="

    ### case insensitive 2001 - 2023:
    # query_url = f"https://www.kielipankki.fi/korp/api8/query?default_context=1%20sentence&show=sentence%2Cparagraph%2Clemma%2Clemmacomp%2Cpos%2Cmsd%2Cdephead%2Cdeprel%2Cref%2Clex%2Cspaces%2Cne_name%2Cne_ex%2Cne_type%2Cne_subtype%2Cne_fulltype%2Cne_placename%2Cne_placename_source%2Cne1_name%2Cne1_ex%2Cne1_type%2Cne1_subtype%2Cne1_fulltype%2Cne1_placename%2Cne1_placename_source%2Cne2_name%2Cne2_ex%2Cne2_type%2Cne2_subtype%2Cne2_fulltype%2Cne2_placename%2Cne2_placename_source%2Cnertag2%2Cnertags2%2Cnerbio2&show_struct=text_title%2Ctext_title_orig%2Ctext_date%2Ctext_time%2Ctext_author%2Ctext_author_orig%2Ctext_author_logged_in%2Ctext_author_nick_registered%2Ctext_topic_names%2Ctext_topic_names_orig%2Ctext_topic_name_top%2Ctext_topic_name_leaf%2Ctext_topic_names_set%2Ctext_topic_nums%2Ctext_topic_nums_set%2Ctext_topic_adultonly%2Ctext_msg_type%2Ctext_empty%2Ctext_id%2Ctext_msg_id%2Ctext_thread_id%2Ctext_thread_start_datetime%2Ctext_comment_id%2Ctext_parent_comment_id%2Ctext_parent_datetime%2Ctext_quoted_comment_id%2Ctext_filename_vrt%2Cparagraph_type%2Csentence_id%2Csentence_sentiment_polarity%2Csentence_polarity%2Ctext_sum_lang%2Cparagraph_sum_lang%2Csentence_lang%2Csentence_lang_conf%2Ctext_thread_closed&start=0&end={max_items}&corpus=S24_2001%2CS24_2002%2CS24_2003%2CS24_2004%2CS24_2005%2CS24_2006%2CS24_2007%2CS24_2008%2CS24_2009%2CS24_2010%2CS24_2011%2CS24_2012%2CS24_2013%2CS24_2014%2CS24_2015%2CS24_2016%2CS24_2017%2CS24_2018%2CS24_2019%2CS24_2020%2CS24_2021%2CS24_2022%2CS24_2023&cqp=%5Bword%20%3D%20%22.*{query}.*%22%20%25c%5D&query_data=&context=&incremental=true&default_within=sentence&within="

    ### case insensitive 2001 - 2017:
    query_url = f"https://www.kielipankki.fi/korp/api8/query?default_context=1%20sentence&show=sentence%2Cparagraph%2Clemma%2Clemmacomp%2Cpos%2Cmsd%2Cdephead%2Cdeprel%2Cref%2Clex%2Cspaces%2Cne_name%2Cne_ex%2Cne_type%2Cne_subtype%2Cne_fulltype%2Cne_placename%2Cne_placename_source%2Cne1_name%2Cne1_ex%2Cne1_type%2Cne1_subtype%2Cne1_fulltype%2Cne1_placename%2Cne1_placename_source%2Cne2_name%2Cne2_ex%2Cne2_type%2Cne2_subtype%2Cne2_fulltype%2Cne2_placename%2Cne2_placename_source%2Cnertag2%2Cnertags2%2Cnerbio2&show_struct=text_title%2Ctext_title_orig%2Ctext_date%2Ctext_time%2Ctext_author%2Ctext_author_orig%2Ctext_author_logged_in%2Ctext_author_nick_registered%2Ctext_topic_names%2Ctext_topic_names_orig%2Ctext_topic_name_top%2Ctext_topic_name_leaf%2Ctext_topic_names_set%2Ctext_topic_nums%2Ctext_topic_nums_set%2Ctext_topic_adultonly%2Ctext_msg_type%2Ctext_empty%2Ctext_id%2Ctext_msg_id%2Ctext_thread_id%2Ctext_thread_start_datetime%2Ctext_comment_id%2Ctext_parent_comment_id%2Ctext_parent_datetime%2Ctext_quoted_comment_id%2Ctext_filename_vrt%2Cparagraph_type%2Csentence_id%2Csentence_sentiment_polarity%2Csentence_polarity%2Ctext_sum_lang%2Cparagraph_sum_lang%2Csentence_lang%2Csentence_lang_conf&start=0&end={max_items}&corpus=S24_2001%2CS24_2002%2CS24_2003%2CS24_2004%2CS24_2005%2CS24_2006%2CS24_2007%2CS24_2008%2CS24_2009%2CS24_2010%2CS24_2011%2CS24_2012%2CS24_2013%2CS24_2014%2CS24_2015%2CS24_2016%2CS24_2017&cqp=%5Bword%20%3D%20%22.*{query}.*%22%20%25c%5D&query_data=&context=&incremental=true&default_within=sentence&within="

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/156.0.0.0 Safari/537.36"
        )
    }

    r = requests.get(query_url, headers=headers)
    data = r.json()

    # save results in a json file
    # try:
    with open(q_json, "w") as f:
        json.dump(data, f)
    print(f"{time.strftime('%H:%M', time.localtime())}: Saved query results to: {q_json}")
    # except:
    #     print(f"Failed to save query results to file {q_json}")


def get_query(query: str, queries_dir):
    """Call save_query() first. Read query results from disk and return as json"""

    q_json = f"{queries_dir}/{query}.json"

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

def get_query_hits(query: str, queries_path: str) -> set[tuple]:
    """
    :param query:
    # :return: set() = {(corpus, thread_id, query), ...}   a set of thread_ids found in query
    """

    # threads = defaultdict(int)
    threads = set()

    # load json
    data = get_query(query, queries_path)

    kwic = data["kwic"]

    for comment in kwic:
        thread_id = comment["structs"]["text_thread_id"].encode("utf-8")  # save as bytes for faster parsing later
        corpus = comment["corpus"]
        threads.add((corpus, thread_id, query))

    return threads

def get_unique_titles(query: str, queries_path: str) -> dict:
    """
    Get all unique titles for each corpus associated with a query
    :param query: get titles associated with this query term
    :return: dict[corpus_name]: {title1, title2,...}
    """

    titles = defaultdict(set)
    data = get_query(query, queries_path)
    kwic = data["kwic"]

    for comment in kwic:
        title = comment["structs"]["text_title"]
        corpus = comment["corpus"]
        titles[corpus].add(title)

    return titles

def get_thread_ids(query: str, queries_dir: str) -> set[bytes]:
    """
    :param query:
    # :return: set() = {thread_id, ...}   a set of thread_ids found in query
    """
    thread_ids = set()

    # load json
    data = get_query(query, queries_dir)

    kwic = data["kwic"]
    for comment in kwic:
        thread_id = comment["structs"]["text_thread_id"].encode("utf-8")  # save as bytes for faster parsing later
        thread_ids.add(thread_id)

    return thread_ids



# vocabularies = defaultdict(  # see dict structure above
#     lambda: defaultdict(
#         lambda: defaultdict(
#             lambda: defaultdict(int)
#         )
#     )
# )
"""
vocabularies = {
    query: {  # query as string
        corpus_name: {  # path as string
            thread_id: {    # byte, you have to do this: "123456".encode("utf-8")
                lemma: count    # byte: int
            }
        }
    }
}
"""
def lemma_count():
    return defaultdict(int)

def thread_level():
    return defaultdict(lemma_count)

def corpus_level():
    return defaultdict(thread_level)

def query_level():
    return defaultdict(corpus_level)

vocabularies = defaultdict(query_level)  # see dict structure above


def scan_corpus(corpus_path: str, thread_ids: set[bytes], zf: zipfile.ZipFile):
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
    print(f"{time.strftime('%H:%M', time.localtime())}: Looking for queries.")
    with zipfile.ZipFile(archive_path, mode="r") as zf:
        names = zf.namelist()[0:17]  # include 2001-2017, exclude LICENSE.txt and README.txt

    # download all queries
    for q in queries:
        save_query(q)
    print(f"{time.strftime('%H:%M', time.localtime())}: Queries finished.")


def get_random_query_results(query: str, result_count: int, query_dir="queries", allow_duplicates=False, random_seed=0) -> list:
    """
    If result_count > total comments in query, return all comments in query.
    :param query:
    :param result_count:
    :param allow_duplicates: Can the same comment appear multiple times in the return values.
    :return: list of "kwic" entries
    """
    random.seed(random_seed)
    data = get_query(query, query_dir)
    hits = len(data["kwic"])

    if allow_duplicates:
        results = []
        for i in range(0, result_count):
            comment_nr = random.randint(0, hits - 1)
            results.append(data["kwic"][comment_nr])
        return results

    if hits <= result_count and not allow_duplicates:
        # return every comment in query
        return list(data["kwic"])

    if not allow_duplicates:
        results = []
        comment_nrs = random.sample(range(hits), result_count)
        for comment_nr in comment_nrs:
            results.append(data["kwic"][comment_nr])
        return results

def create_filtered_query_results(queries: list[str],
                                  labeled_samples,
                                  unfiltered_dir: str = "queries",
                                  filtered_dir: str = "filtered_queries",
                                  topic_relevance_cutoff: float = 0.5
                                  ):
    existing = os.listdir(filtered_dir)

    for query in queries:

        # skip existing filtered files
        if f"{query}.json" in existing:
            print(f"{query}.json already exists in {filtered_dir}")
            continue

        # check that labels exist for the query
        if query not in labeled_samples.keys():
            continue

        filtered_path = os.path.join(filtered_dir, f"{query}.json")

        data = get_query(query, unfiltered_dir)
        new_data = data.copy()
        new_data["kwic"] = []
        for kwic_entry in data["kwic"]:
            # get parent topic
            topic = kwic_entry["structs"]["text_topic_names"].split(" > ")[0]

            relevant = labeled_samples[query][topic][1]
            irrelevant = labeled_samples[query][topic][0]

            if relevant + irrelevant == 0: continue

            topic_relevance = relevant / (relevant + irrelevant)

            if topic_relevance > topic_relevance_cutoff:
                new_data["kwic"].append(kwic_entry)

        # save filtered results
        with open(filtered_path, "w") as f:
            json.dump(new_data, f)
        print(f"{time.strftime('%H:%M', time.localtime())}: Saved query results to: {filtered_path}")





def get_text_from_kwic(kwic_entry) -> str:
    """Comment reconstruction is not fully accurate, sometimes spacing is wrong."""
    text_parts = []
    for token in kwic_entry["tokens"]:
        if token["spaces"] == "_":
            text_parts.append(token["word"] + " ")
        else:
            text_parts.append(token["word"])
    return "".join(text_parts)

    # return " ".join(token["word"] for token in kwic_entry["tokens"])

def get_topics_from_kwic(kwic_entry) -> str:
    return kwic_entry["structs"]["text_topic_names_orig"].split(" > ")

def get_date_from_kwic(kwic_entry) -> str:
    return kwic_entry["structs"]["text_date"]

def save_comments_to_disk(kwic_entires, include_topics=True, directory="comment_samples", file_name="sample.csv"):
    """
    format: date (YYYY-MM-DD); topics; text
    :param kwic_entires:
    :param include_topics:
    :param directory:
    :param file_name:
    :return:
    """
    rows = []
    if include_topics:
        for kwic in kwic_entires:
            rows.append({
                # "date": get_date_from_kwic(kwic),
                "topics": "|".join(get_topics_from_kwic(kwic)),
                "text": get_text_from_kwic(kwic)
            })
    else:
        for kwic in kwic_entires:
            rows.append({
                # "date": get_date_from_kwic(kwic),
                "text": get_text_from_kwic(kwic)
            })

    comments_df = pd.DataFrame(rows)

    os.makedirs(directory, exist_ok=True)
    path = os.path.join(directory, file_name)
    comments_df.to_csv(path, sep=",", index=False)
    print(f"Successfully saved file: {path}")

def get_topic_label_counts_as_dict(path: str) -> {}:
    samples = pd.read_csv(path, sep=",")

    # print(samples.head(5))

    # samples = samples[["topics", "label"]].copy()

    samples["topics"] = samples["topics"].apply(
        lambda x: x.split("|") if x else []
    )
    # print(samples.head(5))

    topic_label_counts = defaultdict(lambda: defaultdict(int))
    for _, row in samples.iterrows():
        label = row["label"]
        for topic in row["topics"]:
            topic_label_counts[topic][label] += 1

    return topic_label_counts

def get_topic_label_counts_as_list(path: str) -> []:
    return sorted(
        get_topic_label_counts_as_dict(path).items(),
        key=lambda x: sum(x[1].values()),
        reverse=True
    )


separator = "___"

def scan_task(args):
    query, path, queries_dir, archive_path = args
    thread_ids = get_thread_ids(query, queries_dir)

    with zipfile.ZipFile(archive_path, mode="r") as zf:
        corpus_vocab = scan_corpus(path, thread_ids, zf)

    # save results
    with open(f"scan_tasks/{queries_dir}{separator}{query}{separator}{path.replace('/', '@')}.pkl", "wb") as f:
        pickle.dump(corpus_vocab, f)

    print(f"{time.strftime('%H:%M', time.localtime())}: Scan complete: {query}, {path[-12:-4]}")

def scan(queries: list, queries_dir: str, archive_path: str):
    print(f"{time.strftime('%H:%M', time.localtime())}: Starting Suomi24-corpus scan.")
    # set up the queue for each query term+corpus combination
    with zipfile.ZipFile(archive_path, mode="r") as zf:
        archive_names = zf.namelist()[0:17]  # include 2001-2017, exclude LICENSE.txt and README.txt

    existing = set(os.listdir("scan_tasks"))

    scan_list = [
        (query, path, queries_dir, archive_path)
        for query, path in product(queries, archive_names)
        if f"{queries_dir}{separator}{query}{separator}{path.replace('/', '@')}.pkl"
           not in existing
    ]

    print(f"Running scan {len(scan_list)} times. This will take around (6-10 min X {len(scan_list)}) / core_count on an SSD. To rerun the same task, delete the .pkl files in scan_tasks/.")

    with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        list(executor.map(scan_task, scan_list))

    for file_name in os.listdir("scan_tasks"):
        dir, que, pat = file_name.split(separator, 2)
        if que not in queries:    # exclude irrelevant queries
            continue
        if dir != queries_dir:   # filtered / unfiltered
            continue

        pat = pat.removesuffix(".pkl").replace('@', '/')

        with open(f"scan_tasks/{file_name}", "rb") as f:
            corpus_vocab = pickle.load(f)

        vocabularies[que][pat] = corpus_vocab

    print(f"{time.strftime('%H:%M', time.localtime())}: All scans complete.")

    return vocabularies

def remove_words(vocabularies_: dict, remove: set):
    remove_byte = set()  # change string to byte format to match types
    for word in remove:
        # encode words in byte format
        remove_byte.add(word.encode("utf-8"))
    for query, corpora in vocabularies_.items():
        for corpus_name, threads in corpora.items():
            for thread_id, lemmas in threads.items():
                threads[thread_id] = {
                    lemma: count
                    for lemma, count in lemmas.items()
                    if lemma not in remove_byte
                }

def get_word_count_evolution(vocabularies_: dict) -> dict[str, dict[str, list[int]]]:
    """
    Returns the evolution of word counts for a query when vocabulary is added up thread by thread.
    :param vocabularies_:
    :return: {"query": {"total_words": [in thread_1, in thread_1&2...], "unique_words": [in thread_1, in thread_1&2...]}}
    """
    counts = dict()
    for query in vocabularies_.keys():
        unique_words = set()
        counts[query] = {"total_words": [0], "unique_words": [0]}  # begin with x=0,y=0
        for threads in vocabularies_[query].values():
            # for corpus_name, threads in corpora.items():
            for lemmas in threads.values():
                words_in_thread = 0
                for lemma, count in lemmas.items():
                    unique_words.add(lemma)
                    words_in_thread += count

                counts[query]["total_words"].append(
                    counts[query]["total_words"][-1] + words_in_thread
                )
                counts[query]["unique_words"].append(len(unique_words))
    return counts

if __name__ == "__main__":
    print("archive_reader.py main()")

    queries = ["deradik", "maltillistu", "polaris", "radik"]
    archive = "suomi24-2001-2017-vrt-v1-3.zip"

    query(queries, archive)
    scan(queries, archive, "queries")