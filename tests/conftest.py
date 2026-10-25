from hypothesis import settings

settings.register_profile("lakestream", deadline=None, max_examples=40)
settings.load_profile("lakestream")
