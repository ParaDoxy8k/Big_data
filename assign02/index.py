from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
from io import StringIO

class MapReduce(MRJob):

    def configure_args(self):
        super(MapReduce, self).configure_args()
        self.add_passthru_arg(
            '--join-type',
            default='inner',
            choices=['inner', 'left_outer', 'right_outer', 'full_outer'],
            help='Type of join to perform (inner, left_outer, right_outer, full_outer)'
        )
        self.add_passthru_arg(
            '--output-file',
            default='join_results.txt',
            help='Output file name for the table results'
        )

    def format_table_row(self, record):
        """Format a record as a table row"""
        return f"{record['status_id']:<25} | {record['status_type']:<8} | {record['status_published']:<18} | {str(record['num_reactions_fb2']):<15} | {str(record['num_reactions_fb3']):<15} | {str(record['num_comments']):<12} | {str(record['num_shares']):<10}"

    def mapper(self, _, line):
        # Skip header line
        if line.strip().startswith('Table,status_id'):
            return
        
        # Parse CSV line
        reader = csv.reader(StringIO(line))
        parts = next(reader)
        
        if len(parts) >= 6:
            table, status_id, status_type, status_published, num_reactions = parts[:5]
            
            # Check if this is from fb_live_thailand2.csv (has num_comments)
            if len(parts) == 6 and table == 'FB2':
                num_comments = parts[5]
                yield status_id, ('fb2', {
                    'table': table,
                    'status_type': status_type,
                    'status_published': status_published,
                    'num_reactions': num_reactions,
                    'num_comments': num_comments
                })
            # Check if this is from fb_live_thailand3.csv (has num_shares)
            elif len(parts) == 6 and table == 'FB3':
                num_shares = parts[5]
                yield status_id, ('fb3', {
                    'table': table,
                    'status_type': status_type,
                    'status_published': status_published,
                    'num_reactions': num_reactions,
                    'num_shares': num_shares
                })

    def reducer(self, status_id, values):
        fb2_data = []
        fb3_data = []

        for tag, data in values:
            if tag == 'fb2':
                fb2_data.append(data)
            elif tag == 'fb3':
                fb3_data.append(data)

        join_type = self.options.join_type

        if join_type == 'inner':
            # Inner join: only records that exist in both datasets
            if fb2_data and fb3_data:
                for fb2_record in fb2_data:
                    for fb3_record in fb3_data:
                        combined_record = {
                            'status_id': status_id,
                            'status_type': fb2_record['status_type'],
                            'status_published': fb2_record['status_published'],
                            'num_reactions_fb2': fb2_record['num_reactions'],
                            'num_reactions_fb3': fb3_record['num_reactions'],
                            'num_comments': fb2_record['num_comments'],
                            'num_shares': fb3_record['num_shares']
                        }
                        yield status_id, self.format_table_row(combined_record)
                        
        elif join_type == 'left_outer':
            # Left outer join: all records from fb2, matching records from fb3
            if fb2_data:
                if fb3_data:
                    for fb2_record in fb2_data:
                        for fb3_record in fb3_data:
                            combined_record = {
                                'status_id': status_id,
                                'status_type': fb2_record['status_type'],
                                'status_published': fb2_record['status_published'],
                                'num_reactions_fb2': fb2_record['num_reactions'],
                                'num_reactions_fb3': fb3_record['num_reactions'],
                                'num_comments': fb2_record['num_comments'],
                                'num_shares': fb3_record['num_shares']
                            }
                            yield status_id, self.format_table_row(combined_record)
                else:
                    for fb2_record in fb2_data:
                        combined_record = {
                            'status_id': status_id,
                            'status_type': fb2_record['status_type'],
                            'status_published': fb2_record['status_published'],
                            'num_reactions_fb2': fb2_record['num_reactions'],
                            'num_reactions_fb3': None,
                            'num_comments': fb2_record['num_comments'],
                            'num_shares': None
                        }
                        yield status_id, self.format_table_row(combined_record)
                        
        elif join_type == 'right_outer':
            # Right outer join: all records from fb3, matching records from fb2
            if fb3_data:
                if fb2_data:
                    for fb2_record in fb2_data:
                        for fb3_record in fb3_data:
                            combined_record = {
                                'status_id': status_id,
                                'status_type': fb2_record['status_type'],
                                'status_published': fb2_record['status_published'],
                                'num_reactions_fb2': fb2_record['num_reactions'],
                                'num_reactions_fb3': fb3_record['num_reactions'],
                                'num_comments': fb2_record['num_comments'],
                                'num_shares': fb3_record['num_shares']
                            }
                            yield status_id, self.format_table_row(combined_record)
                else:
                    for fb3_record in fb3_data:
                        combined_record = {
                            'status_id': status_id,
                            'status_type': fb3_record['status_type'],
                            'status_published': fb3_record['status_published'],
                            'num_reactions_fb2': None,
                            'num_reactions_fb3': fb3_record['num_reactions'],
                            'num_comments': None,
                            'num_shares': fb3_record['num_shares']
                        }
                        yield status_id, self.format_table_row(combined_record)
                        
        elif join_type == 'full_outer':
            # Full outer join: all records from both datasets
            if fb2_data and fb3_data:
                for fb2_record in fb2_data:
                    for fb3_record in fb3_data:
                        combined_record = {
                            'status_id': status_id,
                            'status_type': fb2_record['status_type'],
                            'status_published': fb2_record['status_published'],
                            'num_reactions_fb2': fb2_record['num_reactions'],
                            'num_reactions_fb3': fb3_record['num_reactions'],
                            'num_comments': fb2_record['num_comments'],
                            'num_shares': fb3_record['num_shares']
                        }
                        yield status_id, self.format_table_row(combined_record)
            elif fb2_data and not fb3_data:
                for fb2_record in fb2_data:
                    combined_record = {
                        'status_id': status_id,
                        'status_type': fb2_record['status_type'],
                        'status_published': fb2_record['status_published'],
                        'num_reactions_fb2': fb2_record['num_reactions'],
                        'num_reactions_fb3': None,
                        'num_comments': fb2_record['num_comments'],
                        'num_shares': None
                    }
                    yield status_id, self.format_table_row(combined_record)
            elif fb3_data and not fb2_data:
                for fb3_record in fb3_data:
                    combined_record = {
                        'status_id': status_id,
                        'status_type': fb3_record['status_type'],
                        'status_published': fb3_record['status_published'],
                        'num_reactions_fb2': None,
                        'num_reactions_fb3': fb3_record['num_reactions'],
                        'num_comments': None,
                        'num_shares': fb3_record['num_shares']
                    }
                    yield status_id, self.format_table_row(combined_record)

if __name__ == '__main__':
    MapReduce.run()