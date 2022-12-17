import json
test_json = "./test.json"
with open(test_json) as f:
    launches = json.load(f)
    print(isinstance(launches, list))
    # for launch in launches:
    #     if len(launch["links"]["flickr_images"]) > 0:
    #         print(launch["links"]["flickr_images"])
    images = {launch["mission_name"]:launch["links"]["flickr_images"] for launch in launches if len(launch["links"]["flickr_images"]) > 0}
    for mission_name, images in images.items():
        print(mission_name, images[0])
    
        