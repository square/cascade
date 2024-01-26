from block_cascade import remote


@remote(config_name="hello-world", job_name="cmachak-hello-world")
def test_job():
    print("Hello World")


test_job()
