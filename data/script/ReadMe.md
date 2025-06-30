# 🚀 National Watershed ML Pipeline
*Update: Jun 29/ 2025 by Nathan*
This repository contains a **multi-stage machine learning pipeline** for processing HUC8 watersheds, exporting terrain and satellite data from Google Earth Engine, extracting multi-source satellite patches, and generating local hydrography reference layers.

---

## 📌 Pipeline Overview

The pipeline consists of the following stages:

### 1. **HUC Processing**
- Generates patch center points for each HUC8 watershed.
- Exports full HUC DEM from **Google Earth Engine (GEE)** for local processing.

### 2. **Automated Download from Google Drive**
- Merges and downloads exported files from Google Drive.
- Handles duplicate folders automatically created by GEE exports.

### 3. **Patch Extraction**
- Downloads **multi-source**, **multi-resolution** data patches:
  - DEM
  - Landsat optical & thermal
  - Sentinel-1 SAR

### 4. **Local Reference Data Generation**
- Generates hydrography and **flow direction rasters** aligned with patches.

### 5. **(Optional) Statistics Calculation**
- Computes normalization statistics (mean, std) for each HUC (currently optional).

---

## 📁 Folder Structure

```
national_ml/
├── data/
│   └── script/
│       ├── config.py                   # Pipeline configuration
│       ├── huc_process.py              # Export DEMs & patch centers
│       ├── patch_process.py            # Extract and save patches
│       ├── stats_process.py            # Compute normalization stats (optional)
│       ├── local_reference_process.py  # Generate flow direction & hydrography
│       ├── gdrive_manager.py           # Google Drive integration
│       └── run_pipeline.py             # Main entry point
├── pipeline_output/
│   ├── huc_processing/                 # Contains DEMs and patch centers
│   └── patch_dataset/                  # Contains extracted patch data
└── README.md
```

---

## 🚀 Usage

### 1. Prerequisites

- Python **3.8+**
- Google Earth Engine Python API
- Google Drive API credentials

#### Required Python packages:
```bash
pip install -r requirements.txt
```

---

### 2. Setup

- Place your Google Drive `credentials.json` in the `script/` directory.
- Edit `config.py`:
  - Set target HUC8 IDs
  - Adjust patch size, stride, and export settings as needed.
- *(Optional)* Download NHD shapefiles or GPKGs for local hydrography reference layers.

---

### 3. Run the Pipeline

From the `data/script/` directory, run:
```bash
python run_pipeline.py
```

The pipeline will:
- Generate patch centers and export DEMs from GEE.
- Download and organize exports from Google Drive.
- Extract DEM, Landsat, and SAR patches.
- (Optionally) Generate local hydrography and compute normalization statistics.

---

## 🧰 Troubleshooting

- ✅ Ensure valid GEE and Google Drive credentials.
- 🔍 If you get 0 patch centers, verify geometry alignment with DEM.
- 🧠 Large HUCs = more time, memory, and disk.
- 📂 Use GPKG instead of Shapefile if your NHD data exceeds 2GB or has long field names.

---

## 📒 License

This project is licensed under the [MIT License](LICENSE).


==============================================================

# Data Processing
*Update: Jun 24/ 2025 by Dingqi*

> Check gee_tools.py ->launch_single_data_collector function before run it!


> **Test Mode: Only get one batch & one patch for test. To modify-> see gee_tools.py line 310, line 340**

This is a script used for auto data collection/ patches collection

* Create independent subfolders for each HUC in Google Drive.
* buffer the HUC boundary.
* Create the buffered rectangular bounding box.

Export seven types of data:
1. The buffered polygonal boundary (vector)
2. Rectangular outer frame (vector)
3. DEM (Grid)
4. Landsat optical image (grid)
5. Landsat thermal infrared image (grid)
6. SAR radar image (grid)
7. The flow direction of resampling (grid)

These data will be downloaded to `gee_downloads` folder as default.

For test, we randomly select HUC.



## Before use
### Prepare your python enviroment

Install Google Cloud CLI

``` bash
# get the package 
curl https://sdk.cloud.google.com | bash
```

Install earthengine-api:

``` bash
# GEE API
pip install earthengine-api

# access Google Drive API
pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlibconda in
```

### Authenticate GEE and Google Drive respectively.

#### GEE authenticate:

```bash
earthengine authenticate
```

#### Google Drive API authenticate:
This is to give your Python script the permission to access your Google Drive.

    a. Go to the `Google Cloud Console`.

    b. Create a new project (or select an existing one).

    c. In the left menu, find `APIs & Services` -> `Enabled APIs & services`.

    d. Click `ENABLE APIS AND SERVICES`, search for `Google Drive API` and enable it.

    e. Return to `APIs & Services` -> `Credentials`.

    f. Click `CREATE CREDENTIALS` -> `OAuth client ID`.

    g. If required, configure the OAuth consent screen first. Select `External` and fill in the basic information such as the application name.

    h. Return to the credential creation page and select the application type as `Desktop app`.


 After creation, you will see a client ID and a key. Click the `Download JSON File` button on the right, download this file, rename it `credentials.json`, and then place it in the folder where your Python script is located.
 
 Do not forget to publish the app
 ![alt text](imgs/image.png)


## To get the unlimited Google Drive
> By sharing the Google Cloud Platform (GCP) with the Illinois account, you can gain access to GEE while using an unlimited cloud storage.

1. share your project with illinois account:

![step1](imgs/step1.jpg)

![step2](imgs/step2.jpg)

2. create a folder in your illinois account and share it to your own google account

![step3](imgs/step3.jpg)

##  New Features/ Notes

1. Move original HUC boundaries to HUC folder

2. Create tools set for visualization patch center and patch size. setting in config.py VISUALIZE_POINTS. 


3. Get patches and auto downloads(Highly recommend to run only a portion of the code during testing (comment out the rest in gee_tools), as running the full workflow can be time-consuming.)

4. Manage your task though: https://code.earthengine.google.com/tasks

5. Dont do getInfo() for the whole FeatureCollection, it will take long long time.

6. All image will be reprojected to DEM projection('EPSG:4269'). All projection will be printed. Make sure they are same.

---
* Todo: Use map() than `for` loop to speed up




## Additional Resources

For the record, I created a Google Drive folder for backup purposes. You can access it using the following link:

[Google Drive Folder](https://drive.google.com/drive/folders/13b4g6TtqcbOZp2pKSxqs71IV1oRF6NAq?usp=sharing) 



