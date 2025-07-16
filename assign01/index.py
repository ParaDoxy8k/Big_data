from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
from datetime import datetime

class Top2StatusPerYear(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_extract,
                   reducer=self.reducer_count),
            MRStep(mapper=self.mapper_passthrough,
                   reducer=self.reducer_top2_format),
            MRStep(mapper=self.mapper_passthrough2,
                   reducer=self.reducer_sort_years)
        ]

    def mapper_extract(self, _, line):
        try:
            fields = list(csv.reader([line]))[0]
            if fields[0] == 'status_id':
                return  # skip header

            status_type = fields[1].lower()
            date_str = fields[2]
            year = datetime.strptime(date_str, '%m/%d/%Y %H:%M').year

            if status_type in ['photo', 'video', 'link', 'status']:
                yield (year, status_type), 1
        except:
            pass

    def reducer_count(self, key, values):
        year, status_type = key
        yield year, (status_type, sum(values))

    def mapper_passthrough(self, year, type_count):
        yield year, type_count

    def reducer_top2_format(self, year, type_counts):
        top2 = sorted(type_counts, key=lambda x: -x[1])[:2]
        formatted = ', '.join(f"{t} ({c})" for t, c in top2)
        yield None, (year, formatted)

    def mapper_passthrough2(self, _, year_formatted):
        yield None, year_formatted

    def reducer_sort_years(self, _, year_formatted_pairs):
        sorted_pairs = sorted(year_formatted_pairs, key=lambda x: -x[0])
        for year, formatted in sorted_pairs:
            yield year, formatted

if __name__ == '__main__':
    Top2StatusPerYear.run()