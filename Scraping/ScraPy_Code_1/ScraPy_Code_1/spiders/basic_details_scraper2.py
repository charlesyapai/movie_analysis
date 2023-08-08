import scrapy
import csv
import os
import logging

class IMDbMovieSpider(scrapy.Spider):
    name = 'imdb_basic_details_spider2'
    base_url = 'https://www.imdb.com/search/title/?title_type=feature&user_rating=1.0,10.0&languages=en&sort=boxoffice_gross_us,desc'
    output_directory = '/content/drive/MyDrive/IMDB Project/Scraping/scraped_data/ScraPy_Code_1.1_data'
    os.makedirs(output_directory, exist_ok=True)

    def start_requests(self):
        
        print("STARTING START REQUESTSSSS\n\n\n")
        
        # # Loop through 2-year intervals
        # for year in range(1996,2023,2):
        #     start_date = f'{year-1}-01-01'
        #     end_date = f'{year}-12-31'
        #     url = f'{self.base_url}&release_date={start_date},{end_date}&start=1'
        #     output_file = os.path.join(self.output_directory, f'movies_{year-1}_{year}.csv')
        
        
        # Set year to 2023
        year = 2023
        start_date = f'{year}-01-01'
        end_date = f'{year}-12-31'
        url = f'{self.base_url}&release_date={start_date},{end_date}&start=1'
        output_file = os.path.join(self.output_directory, f'movies_{year}.csv')

    
        # Log the URL and output file
        self.log(f'URL: {url}', level=logging.INFO)
        self.log(f'Output file: {output_file}', level=logging.INFO)

        # Write the headers to the file
        with open(output_file, 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file, delimiter='|')
            writer.writerow(['Title', 'Gross', 'Details URL', 'Genres'])
            
        # Send request for each 2-year interval
        yield scrapy.Request(url=url, callback=self.parse, meta={'output_file': output_file, 'start_year': year-1})
    
    def parse(self, response):
        print("STARTING PARSE \n\n\n\n")
        
        self.log('Visited %s' % response.url)
        
        output_file = response.meta['output_file']
        start_year = response.meta['start_year']

        for movie in response.css('div.lister-item'):
            title = movie.css('h3.lister-item-header a::text').get()
            gross = movie.css('p.sort-num_votes-visible span[name="nv"]:last-child::attr(data-value)').get()
            details_url = movie.css('h3.lister-item-header a::attr(href)').get()
            genres = movie.css('span.genre::text').get()

    
            
            # Count the number of rows in the file
            with open(output_file, 'r', newline='', encoding='utf-8') as file:
                reader = csv.reader(file, delimiter='|')
                row_count = sum(1 for row in reader)

            # Save to the appropriate CSV file
            with open(output_file, 'a', newline='', encoding='utf-8') as file:
                writer = csv.writer(file, delimiter='|')
                writer.writerow([title, gross, details_url, genres])
                

            # Print the number of rows saved to the file
            print(f'Saved {row_count} rows to {output_file}')

    

        # Pagination
        current_start = int(response.url.split('&start=')[1].split('&')[0])

        # Stop if 10000 titles are reached for the current 2-year interval
        if current_start >= 10000:
            return

        # Go to the next page
        next_start = current_start + 50
        next_page = f'{self.base_url}&release_date={start_year}-01-01,{start_year+2}-12-31&start={next_start}'
        yield scrapy.Request(url=next_page, callback=self.parse, meta={'output_file': output_file, 'start_year': start_year})
