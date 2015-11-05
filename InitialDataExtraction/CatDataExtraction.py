from bs4 import BeautifulSoup
import httplib2

def main():
    category_list = []
    h = httplib2.Http('.cache')
    websites = ["http://www.goodreads.com/genres/list?page=1", "http://www.goodreads.com/genres/list?page=2",
                "http://www.goodreads.com/genres/list?page=3"]
    for website in websites:
        response, content = h.request(website)
        if response.status != 200:
            print("Status code ", response.status)
            return
        soup = BeautifulSoup(content, 'html.parser')
        data = soup.find_all("a", class_="mediumText actionLinkLite")
        for x in data:
            category_list.append(str(x.string))

    data = "category_list = " + str(category_list)

    with open("InitialDataExtraction/category_list.py", mode='w', encoding="utf-8") as a_file:
        a_file.write(data)
    print(len(category_list))
if __name__ == '__main__':
    main()