import scrapy
import csv
import os

class IMDbMovieSpider(scrapy.Spider):
    name = 'imdb_movie_spider_v4'
    base_url = 'https://www.imdb.com/search/title/?title_type=feature&user_rating=1.0,10.0&languages=en&sort=boxoffice_gross_us,desc'
    output_directory = '/content/drive/MyDrive/IMDB Project/Scraping/scraped_data/ScraPy_Code_1_data'
    os.makedirs(output_directory, exist_ok=True)

    def start_requests(self):
        # Loop through 2-year intervals
        for year in range(2023, 1980, -2):
            start_date = f'{year-1}-01-01'
            end_date = f'{year}-12-31'
            url = f'{self.base_url}&release_date={start_date},{end_date}&start=1'
            output_file = os.path.join(self.output_directory, f'movies_{year-1}_{year}.csv')
            

        # Write the headers to the file
            with open(output_file, 'w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file, delimiter='|')
                writer.writerow(['Title', 'Image URL', 'Details URL', 'Date', 'Duration', 'Genres', 'Rating', 'Metascore', 'Summary', 'Votes', 'Gross'])
            
            # Send request for each 2-year interval
            yield scrapy.Request(url=url, callback=self.parse, meta={'output_file': output_file, 'start_year': year-1})
    
    def parse(self, response):
        output_file = response.meta['output_file']
        start_year = response.meta['start_year']

        for movie in response.css('div.lister-item'):
            title = movie.css('h3.lister-item-header a::text').get()
            image_url = movie.css('img.loadlate::attr(src)').get()
            date = movie.css('h3.lister-item-header span.lister-item-year::text').get()
            details_url = movie.css('h3.lister-item-header a::attr(href)').get()
            duration = movie.css('span.runtime::text').get()
            genres = movie.css('span.genre::text').get()
            rating = movie.css('div.ratings-imdb-rating strong::text').get()
            metascore = movie.css('span.metascore::text').get()
            summary = movie.css('p.text-muted:nth-child(4)::text').get().strip()
            votes = movie.css('p.sort-num_votes-visible span[name="nv"]:first-child::attr(data-value)').get()
            gross = movie.css('p.sort-num_votes-visible span[name="nv"]:last-child::attr(data-value)').get()
            
            # Save to the appropriate CSV file
            with open(output_file, 'a', newline='', encoding='utf-8') as file:
                writer = csv.writer(file, delimiter='|')
                writer.writerow([title, image_url, details_url, date, duration, genres, rating, metascore, summary, votes, gross])
            # Count the number of rows in the file
            with open(output_file, 'r', newline='', encoding='utf-8') as file:
                reader = csv.reader(file, delimiter='|')
                row_count = sum(1 for row in reader)

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
