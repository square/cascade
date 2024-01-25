from block_cascade import remote

@remote(job_name="cmachak-pr-test2")
def test_job():
    print("Hello World")


test_job()
