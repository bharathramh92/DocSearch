from Collections import get_files_in_input_dir
import json


def main():
    input_file_list = get_files_in_input_dir()

    item_ids, total_items, total_duplicates = set(), 0, 0
    for fl in input_file_list:
        if fl[0] == '.':        # avoiding hidden file
            continue
        out_data = []
        with open("input/" + fl, mode='r', encoding='utf-8') as a_file:
            for line in a_file.readlines():
                temp_json = json.loads(line)
                for data in temp_json:
                    if data['id'] not in item_ids:
                        total_items += 1
                        item_ids.add(data['id'])
                        out_data.append(data)
                    else:
                        total_duplicates += 1
        if len(out_data) > 0:
            with open("input_sanitized/data", mode='a', encoding='utf-8') as a_file:
                for data in out_data:
                    a_file.write(str(data) + "\n")

    print("Total duplicates found were %d, and total items were %d" %(total_duplicates, total_items))

if __name__ == '__main__':
    main()