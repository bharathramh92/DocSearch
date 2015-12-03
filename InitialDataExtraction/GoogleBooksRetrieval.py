from category_list import category_list
from Collections import get_files_in_input_dir
from urllib.parse import urlencode
import httplib2
import json
import time


def main():
    # Since google blocks our ip due excessive requests, data has to be retrieved on a sequential basis.
    # This method generates a file in "input" directory for each search term
    # Search term are to be provided in category_list.py as list

    maxResults = 40     # 40 is the maximum number of records that could be extracted from google books api.

    google_api = "https://www.googleapis.com/books/v1/volumes"
    get_params = {'langRestrict': 'en', 'maxResults': maxResults, 'printType': 'books', 'orderBy': 'newest'}

    completed_items_set = set()
    output_list = []

    retry_counter, retry_limit, retry_time_seconds = 0, 100, \
                                                     int(input("What should the retry interval be in seconds?\n"))

    try:
        # completed_items_checklist has the list of ids of books whose data are downloaded
        with open("completed_items_checklist", mode='r', encoding='utf-8') as a_file:
            for line in a_file.readlines():
                completed_items_set.add(line)
    except FileNotFoundError:
        pass
    print("Total books retrieved so far %d" % (len(completed_items_set)))

    def write_to_completed_items_checklist(data):
        with open("completed_items_checklist", mode='a', encoding='utf-8') as a_file:
            a_file.write(data + "\n")

    completed_categories = get_files_in_input_dir()

    h = httplib2.Http(".cache")

    num_char_dict = {0:'th', 1:'st', 2:'nd', 3:'rd', 4:'th', 5:'th', 6:'th', 7:'th', 8:'th', 9:'th'}

    def request_data(startIndex):
        nonlocal retry_counter, retry_limit, retry_time_seconds, num_char_dict
        get_params['startIndex'] = startIndex
        response, content = h.request(google_api + "?" + urlencode(get_params))
        if response.status != 200:
            # if google blocks our ip, the following message would be displayed
            # userRateLimitExceededUnreg
            if "userRateLimitExceededUnreg" in str(content) and retry_counter < retry_limit:
                retry_counter += 1
                print("Waiting to get the hold lifted from Google")
                # waiting for google to lift the hold
                time.sleep(retry_time_seconds)

                print("Retrying for the %d'%s time" %(retry_counter, num_char_dict[retry_counter % 10]))
                request_data(startIndex)
            else:
                print("Response to google api was %s.\nContent is %s" %(response, content))
                return 1
        obj = json.loads(content.decode("utf-8"))
        try:
            obj['items']
        except KeyError:
            return None
        return obj

    required_data = ['title', 'subtitle', 'authors', 'categories', 'description', 'averageRating', 'imageLinks',
                     'pageCount', 'publisher', 'infoLink', 'maturityRating', 'industryIdentifiers']

    for category in category_list:
        if category in completed_categories:
            continue

        print("Querying for ", category)

        get_params['q'] = category
        index_counter = 0
        data = request_data(index_counter)

        while data is not None:
            if data == 1:
                return
            for item in data['items']:
                temp_item_dict = {}
                temp_item_dict['id'] = item['id']
                if temp_item_dict['id'] in completed_items_set:
                    continue
                for param in required_data:
                    try:
                        temp_item_dict[param] = item['volumeInfo'][param]
                    except KeyError as k:
                        pass

                    if temp_item_dict.get('industryIdentifiers') is not None:
                        for identifier in temp_item_dict['industryIdentifiers']:
                                try:
                                    temp_item_dict[identifier['type']] = identifier['identifier']
                                except KeyError as k:
                                    pass
                                    # print("Couldn't get isbn ")
                        temp_item_dict.pop('industryIdentifiers')
                output_list.append(temp_item_dict)
                completed_items_set.add(temp_item_dict['id'])
                write_to_completed_items_checklist(temp_item_dict['id'])
            index_counter += len(data['items'])
            print("Number of retrieved items %d" % len(output_list), end='\r')
            data = request_data(index_counter)
        out_data = json.dumps(output_list)
        print("", end="\r")
        with open("input/" + category, mode='w', encoding='utf-8') as a_file:
            print("Writing data in input/", category)
            a_file.write(out_data)
        completed_categories.append(category)
        print("Finished %.2f%%" % (100*len(completed_categories)/len(category_list)))

if __name__ == '__main__':
    main()
