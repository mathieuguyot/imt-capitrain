class TimeExchangeData:

    def __init__(self, job_id, from_storage_id, to_storage_id):
        self.job_id = job_id
        self.from_storage_id = from_storage_id
        self.to_storage_id = to_storage_id
        self.time = 0