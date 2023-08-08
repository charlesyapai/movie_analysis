
import pandas as pd
import scrapy
import datetime
import json
import os
from scrapy import signals
import csv


class csv_dialect(csv.Dialect):
    delimiter = ','
    quotechar = '"'
    doublequote = True
    skipinitialspace = False
    lineterminator = '\n'
    quoting = csv.QUOTE_ALL


class IMDbSpider(scrapy.Spider):
    name = 'scraping_test2'
    user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    output_directory = '/content/drive/MyDrive/IMDB Project/Scraping/scraped_data/ScraPy_Code_2.2_data'
    os.makedirs(output_directory, exist_ok=True)
    data = []

    # Try to read in the already scraped titles
    try:
        df_scraped = pd.read_csv('/content/drive/MyDrive/IMDB Project/Scraping/scraped_data/ScraPy_Code_2.2_data/more_details.csv', header=None)
    except pd.errors.EmptyDataError:
        # If the file is empty, create an empty DataFrame
        df_scraped = pd.DataFrame()

    scraped_ids = df_scraped[0].tolist() if not df_scraped.empty else []

    # Read in the start URLs and remove already scraped titles
    df = pd.read_csv('/content/drive/MyDrive/IMDB Project/Scraping/scraped_data/ScraPy_Code_1_data/all_movie_ids_final.csv')
    df['details_url'] = 'https://www.imdb.com' + df['details_url']
    df['id'] = df['details_url'].str.extract(r'(tt\d+)')
    df = df[~df['id'].isin(scraped_ids)]
    start_urls = df['details_url'].tolist()

    # Record the start time
    start_time = datetime.datetime.now()

    # Initialize a counter
    url_count = 0

    def handle_error(self, failure):
        self.log(failure)

    def start_requests(self):
        for url in self.start_urls:
            imdb_id = url.split('/')[-2]
            output_file = os.path.join(self.output_directory, 'more_details_2.csv')
            yield scrapy.Request(url, headers={'User-Agent': self.user_agent}, meta={'imdb_id': imdb_id}, errback=self.handle_error, callback=self.parse)

    def parse(self, response):
        imdb_id = response.meta['imdb_id']


        # Increment the counter
        self.url_count += 1



        # Calculate the elapsed time and the average time per item
        elapsed_time = datetime.datetime.now() - self.start_time
        avg_time_per_item = elapsed_time / self.url_count

        # Estimate the remaining time
        remaining_items = len(self.start_urls) - self.url_count
        estimated_remaining_time = avg_time_per_item * remaining_items

        self.log(f'***********************TIME ESTIMATION ********************************* \n\n Processing {imdb_id} ({self.url_count}/{len(self.start_urls)}), estimated remaining time: {estimated_remaining_time}\n\n')




        # Initialize the meta dictionary with the imdb_id
        meta = {
            'imdb_id': imdb_id
        }
    #START OF PARSING CODE HERE 
    #Scraping Main Details
        title = response.css('h1.sc-afe43def-0 span.sc-afe43def-1::text').get()
        director = response.css('li[data-testid="title-pc-principal-credit"] span:contains("Director") ~ div ul li a::text').get()
        writers = response.css('li[data-testid="title-pc-principal-credit"] span:contains("Writers") ~ div ul li a::text').getall()
        stars = response.css('li[data-testid="title-pc-principal-credit"] a:contains("Stars") ~ div ul li a::text').getall()
        user_reviews = response.css('ul[data-testid="reviewContent-all-reviews"] a:contains("User reviews") span.score::text').get()
        critic_reviews = response.css('ul[data-testid="reviewContent-all-reviews"] a:contains("Critic reviews") span.score::text').get()
        metascore = response.css('ul[data-testid="reviewContent-all-reviews"] a:contains("Metascore") span.score-meta::text').get()

    # Scraping Technical Specs
        tech_specs = response.css('div[data-testid="title-techspecs-section"]')
        runtime = tech_specs.css('li[data-testid="title-techspec_runtime"] div.ipc-metadata-list-item__content-container::text').getall()
        runtime = " ".join(runtime)  # Joining the scraped parts to form the complete runtime text
        sound_mix = tech_specs.css('li[data-testid="title-techspec_soundmix"] a::text').getall()
        # Scraping aspect ratio
        aspect_ratio = tech_specs.css('span.ipc-metadata-list-item__list-content-item::text').get()
    # Scraping Box Office Information
        budget = response.css('li[data-testid="title-boxoffice-budget"] span.ipc-metadata-list-item__list-content-item::text').get()
        gross_us_canada = response.css('li[data-testid="title-boxoffice-grossdomestic"] span.ipc-metadata-list-item__list-content-item::text').get()
        
        # 
        opening_weekend_data = response.css('li[data-testid="title-boxoffice-openingweekenddomestic"] span.ipc-metadata-list-item__list-content-item::text').getall()
        if opening_weekend_data:
            opening_weekend_amount = opening_weekend_data[0]
            opening_weekend_date = opening_weekend_data[1] if len(opening_weekend_data) > 1 else None
        else:
            opening_weekend_amount = None
            opening_weekend_date = None
        # 
        
        
        
        # opening_weekend_amount = response.css('li[data-testid="title-boxoffice-openingweekenddomestic"] span.ipc-metadata-list-item__list-content-item::text').getall()[0]
        # opening_weekend_date = response.css('li[data-testid="title-boxoffice-openingweekenddomestic"] span.ipc-metadata-list-item__list-content-item::text').getall()[1]
        opening_weekend_us_canada = f"{opening_weekend_amount}, {opening_weekend_date}"
        gross_worldwide = response.css('li[data-testid="title-boxoffice-cumulativeworldwidegross"] span.ipc-metadata-list-item__list-content-item::text').get()

    # Scraping Details Section
        release_date = response.css('li[data-testid="title-details-releasedate"] a.ipc-metadata-list-item__list-content-item--link::text').get()
        countries_of_origin = response.css('li[data-testid="title-details-origin"] a.ipc-metadata-list-item__list-content-item--link::text').getall()
        official_sites = response.css('li[data-testid="details-officialsites"] a.ipc-metadata-list-item__list-content-item--link::attr(href)').getall()
        languages = response.css('li[data-testid="title-details-languages"] a.ipc-metadata-list-item__list-content-item--link::text').getall()
        also_known_as = response.css('li[data-testid="title-details-akas"] span.ipc-metadata-list-item__list-content-item::text').get()
        filming_locations = response.css('li[data-testid="title-details-filminglocations"] a.ipc-metadata-list-item__list-content-item--link::text').get()
        production_companies = response.css('li[data-testid="title-details-companies"] a.ipc-metadata-list-item__list-content-item--link::text').getall()


        # Update the meta dictionary with new data
        meta.update({
            'title': title,
            'runtime': runtime,
            'sound_mix': sound_mix,
            'aspect_ratio': aspect_ratio,
            'budget': budget,
            'gross_us_canada': gross_us_canada,
            'opening_weekend_us_canada': opening_weekend_us_canada,
            'gross_worldwide': gross_worldwide,
            'writers': writers,
            'release_date': release_date,
            'countries_of_origin': countries_of_origin,
            'official_sites': official_sites,
            'director': director,
            'writers': writers,
            'stars': stars,
            'user_reviews': user_reviews,
            'critic_reviews': critic_reviews,
            'metascore': metascore,
            'languages': languages,
            'also_known_as': also_known_as,
            'filming_locations': filming_locations,
            'production_companies': production_companies
        })


        print(f'Title: {title}')
        print(f'Director: {director}')
        
        yield response.follow(f'https://www.imdb.com/title/{imdb_id}/plotsummary', self.parse_plot_summary, meta=meta, errback=self.handle_error)

    def parse_plot_summary(self, response):
        imdb_id = response.meta['imdb_id']

        # Scraping plot summaries
        plot_summaries = response.css('div[data-testid="sub-section-summaries"] div.ipc-html-content-inner-div::text').getall()
        # Scraping synopsis
        synopsis = response.css('ul.meta-data-list-full div.ipc-html-content-inner-div::text').get()



        # Update the meta dictionary with new data
        response.meta.update({
            'plot_summaries': plot_summaries,
            'synopsis': synopsis
        })


        yield response.follow(f'https://www.imdb.com/title/{imdb_id}/reviews?ref_=tt_urv', self.parse_user_reviews, meta=response.meta, errback=self.handle_error)


    def parse_user_reviews(self, response):
        imdb_id = response.meta['imdb_id']
        review_blocks = response.css('.review-container')

        if not review_blocks:
            print("No review blocks found.")
            print("Going to Technical Specs")
            yield response.follow(f'https://www.imdb.com/title/{imdb_id}/technical/?ref_=tt_spec_sm', 
                                self.parse_technical_specs, 
                                meta=response.meta, 
                                errback=self.handle_error)

        # Get reviews_data from response.meta, if it doesn't exist initialize it as an empty list
        reviews_data_str = response.meta.get('reviews_data', '[]')
        # Convert the string back to a list
        reviews_data_list = eval(reviews_data_str)
        reviewer_ratings = response.meta.get('reviewer_ratings', [])

        for block in review_blocks:
            review = block.css('.text.show-more__control::text').get()
            reviewer = block.css('.display-name-link a::text').get()
            rating = block.css('.ipl-ratings-bar span.rating-other-user-rating span::text').get()

            print(f"Reviewer: {reviewer}, Rating: {rating}")

            review_data = {  # dictionary to hold individual review data
                'review': review,
                'reviewer': reviewer,
                'rating': rating
            }

            reviews_data_list.append(str(review_data))  # append string representation of review_data dictionary to reviews_data_list

            reviewer_ratings.append({
                'reviewer': reviewer,
                'rating': rating
            })

        # Convert the list back to a string to store it in response.meta
        reviews_data_str = str(reviews_data_list)

        response.meta.update({
            'reviews_data': reviews_data_str,  # Update with the string representation of the list
            'reviewer_ratings': reviewer_ratings
        })

        key = response.css('.load-more-data::attr(data-key)').get()

        # ... rest of your code


        print(f"Key: {key}")

        if key:
            yield scrapy.Request(
                url = f'https://www.imdb.com/title/{imdb_id}/reviews/_ajax?ref_=undefined&paginationKey='+ key,
                callback=self.parse_user_reviews, 
                meta=response.meta,  
            )
        else:
            print("No key found.")
            yield response.follow(f'https://www.imdb.com/title/{imdb_id}/technical/?ref_=tt_spec_sm', 
                                self.parse_technical_specs, 
                                meta=response.meta, 
                                errback=self.handle_error)

    def parse_technical_specs(self, response):
        imdb_id = response.meta['imdb_id']

        # Scraping Technical Specs
        runtime = response.css('li#runtime span.ipc-metadata-list-item__list-content-item::text').getall()
        sound_mix = response.css('li#soundmixes a.ipc-metadata-list-item__list-content-item--link::text').getall()
        color = response.css('li#colorations a.ipc-metadata-list-item__list-content-item--link::text').getall()
        aspect_ratio = response.css('li#aspectratio span.ipc-metadata-list-item__list-content-item::text').getall()
        camera = response.css('li#cameras span.ipc-metadata-list-item__list-content-item::text').getall()
        laboratory = response.css('li#laboratory span.ipc-metadata-list-item__list-content-item::text').getall()
        film_length = response.css('li#filmLength span.ipc-metadata-list-item__list-content-item::text').getall()
        negative_format = response.css('li#negativeFormat span.ipc-metadata-list-item__list-content-item::text').getall()
        cinematographic_process = response.css('li#process span.ipc-metadata-list-item__list-content-item::text').getall()
        printed_film_format = response.css('li#printedFormat span.ipc-metadata-list-item__list-content-item::text').getall()

        

        # Update the meta dictionary with new data
        response.meta.update({
            'runtime': runtime,
            'sound_mix': sound_mix,
            'color': color,
            'aspect_ratio': aspect_ratio,
            'camera': camera,
            'laboratory': laboratory,
            'film_length': film_length,
            'negative_format': negative_format,
            'cinematographic_process': cinematographic_process,
            'printed_film_format': printed_film_format
        })

        yield response.follow(f'https://www.imdb.com/title/{imdb_id}/externalreviews?ref_=tt_ov_rt', self.parse_external_reviews, meta=response.meta, errback=self.handle_error)

    def parse_external_reviews(self, response):
        imdb_id = response.meta['imdb_id']

        # Locate all review site blocks
        review_site_blocks = response.css('.ipc-metadata-list__item.ipc-metadata-list-item--link')

        # Lists to store review site names and URLs
        review_site_names = []
        review_site_urls = []

        # For each block, extract the reviewer site name and URL
        for block in review_site_blocks:
            review_site_name = block.css('a.ipc-metadata-list-item__label--link::text').get()
            review_site_url = block.css('a.ipc-metadata-list-item__label--link::attr(href)').get()

            # Add reviewer site name to the list
            review_site_names.append(review_site_name)

            # Add reviewer site URL to the list
            review_site_urls.append(review_site_url)

        # Update the meta dictionary with new data
        response.meta.update({
            'review_site_names': review_site_names,
            'review_site_urls': review_site_urls
        })


        # Convert the meta dictionary to a DataFrame
        meta_df = pd.DataFrame([response.meta])

        # Write to CSV file
        output_file = os.path.join(self.output_directory, '/content/drive/MyDrive/IMDB Project/Scraping/scraped_data/ScraPy_Code_2.2_data/more_details.csv')
        print(f"Writing data to {output_file}")  # Print the file path
        print(meta_df)  # Print the data that is being written
        meta_df.to_csv(output_file, mode='a', header=False, index=False, encoding='utf-8')

        # Yielding the scraped data
        yield response.meta

    def handle_error(self, failure): 
        # Log all failures
        self.log(failure)
        # Yield the meta data
        yield failure.request.meta