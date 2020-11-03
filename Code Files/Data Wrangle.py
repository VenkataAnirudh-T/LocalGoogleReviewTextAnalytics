import json

# Parameters
reviewsfileloc = "D:\\670\\GoogleReviewsData\\users.json"


# Read the reviews file
reviewsfile = open(reviewsfileloc)
reviewjsondata = json.load(reviewsfile)

print(reviewjsondata)