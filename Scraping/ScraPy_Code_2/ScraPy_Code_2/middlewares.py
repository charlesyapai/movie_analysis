import time
from scrapy.exceptions import IgnoreRequest

class Retry503Middleware:
    def process_response(self, request, response, spider):
        if response.status == 503:
            time.sleep(10 * 60)  # Sleep for 10 minutes
            return request  # Retry the request
        return response

    def process_exception(self, request, exception, spider):
        if isinstance(exception, IgnoreRequest):
            time.sleep( * 60)  # Sleep for 20 minutes
            return request  # Retry the request