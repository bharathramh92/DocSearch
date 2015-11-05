from category_list import category_list
from urllib.parse import urlencode
import httplib2
import json
import time

def main():

    maxResults = 40

    google_api = "https://www.googleapis.com/books/v1/volumes"
    get_params = {'langRestrict': 'en', 'maxResults': maxResults, 'printType': 'books', 'orderBy': 'newest'}


    output_list = []
    retry_counter, retry_limit, retry_time_seconds = 0, 100, 360

    # completed_list_copy = deepcopy(completed_list)
    def get_input_file():
        import sys
        from os import walk
        path = sys.argv[0].split('/')
        path.pop(len(path)-1)
        path.append("input")
        path = '/'.join(path)
        f = []
        for (dirpath, dirnames, filenames) in walk(path):
            f.extend(filenames)
        return f

    completed_categories = get_input_file()

    h = httplib2.Http(".cache")

    def request_data(startIndex):
        nonlocal retry_counter, retry_limit, retry_time_seconds
        get_params['startIndex'] = startIndex
        response, content = h.request(google_api + "?" + urlencode(get_params))
        if response.status != 200:
            if "userRateLimitExceededUnreg" in str(content) and retry_counter < retry_limit:
                retry_counter += 1
                time.sleep(retry_time_seconds)
                print("Retrying for the %d time" %(retry_counter))
                request_data(startIndex)
            else:
                print("Response to google api was %s.\nContent is %s" %(response, content))
                return 1
        obj = json.loads(content.decode("utf-8"))
        # print("Total items %d.\nCurrent retrieved contents %d" %(obj['totalItems'], len(obj['items'])))
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
                for param in required_data:
                    try:
                        temp_item_dict[param] = item['volumeInfo'][param]
                    except KeyError as k:
                        pass
                        # print("Couldn't find ", param)

                    if temp_item_dict.get('industryIdentifiers') is not None:
                        for identifier in temp_item_dict['industryIdentifiers']:
                                try:
                                    temp_item_dict[identifier['type']] = identifier['identifier']
                                except KeyError as k:
                                    pass
                                    # print("Couldn't get isbn ")
                        temp_item_dict.pop('industryIdentifiers')
                output_list.append(temp_item_dict)
            index_counter += maxResults
            print("Number of retrieved items %d" %len(output_list), end='\r')
            data = request_data(index_counter)

        out_data = json.dumps(output_list)
        # print(out_data)
        with open("input/" + category, mode='w', encoding='utf-8') as a_file:
            a_file.write(out_data)

    # index_counter = 0
    # get_params['q'] = "algorithm"
    # data = request_data(index_counter)
    #
    # while data is not None:
    #     for item in data['items']:
    #         temp_item_dict = {}
    #         temp_item_dict['id'] = item['id']
    #         for param in required_data:
    #             try:
    #                 temp_item_dict[param] = item['volumeInfo'][param]
    #             except KeyError as k:
    #                 print("could find " , param)
    #         output_list.append(temp_item_dict)
    #     index_counter += maxResults
    #     print("Number of retrieved items %d" %len(output_list))
    #     data = request_data(index_counter)
    #     break

    # print(output_list)

if __name__ == '__main__':
    main()
