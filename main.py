import runpy
import sys

modules = [
    "./lab100_orders/orderStatisticsApp.py",
    "./lab300_nyc_school_stats/newYorkSchoolStatisticsApp.py",
    "./lab400_udaf/pointsPerOrderApp.py",
    "./lab900_max_value/maxValueAggregationApp.py",
    "./lab910_nyc_to_postgresql/newYorkSchoolsToPostgreSqlApp.py",
    "./lab920_count_books_per_author/authorsAndBooksCountBooksApp.py",
    "./lab930_aggregation_as_list/authorsAndListBooksApp.py"
]

if __name__ == '__main__':

    for i, module in enumerate(modules):
        print("{0} {1}".format(i, module))
    print("")

    test_text = input("Введите вариант : ")
    module_number = int(test_text)

    runpy.run_path(modules[module_number], run_name='__main__')
