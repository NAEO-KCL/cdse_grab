# Copernicus Data Storage Ecosystem grabber

The CDSE seems to be the new way to access Copernicus data. Here's a simple wrapper library to search and turn it into something useful.

## Account configuration
You will need [an account](https://eodata-s3keysmanager.dataspace.copernicus.eu/) as well as an S3 token. There are instructions for this [here](https://documentation.dataspace.copernicus.eu/APIs/S3.html#registration). This will result in two bits of information that you'll need to note down and then store in a file 
in `~/.sentinel3/config.json`. The contents of said file should be
```
{
    "s3": {
        "endpoint_url": "eodata.ams.dataspace.copernicus.eu",
        "access_key": "YOUR_ACCESS_KEY_HERE",
        "secret_key": "YOUR_SECRET_KEY_HERE",
        "https": true
    },
    "stac": {
        "catalogue_url": "https://stac.dataspace.copernicus.eu/v1"
    },
    "logging": {
        "level": "INFO"
    }
}
```
**NOTE** I'm not even sure if that's the right endpoint, but seems to work with Sentinel 3 data ;-)

## Installing

You can install directly from here using
```
pip install git+https://github.com/NAEO-KCL/cdse_grab
```

You can also clone or download the respository and install using e.g.

```
pip install -e .
```

You can then import the library using `import cdse_grab`.

Note that this will pull other libraries if you haven't got them (fsspec, xarray, pandas...)

## Example notebook
There's an example notebook in the `notebooks` folder
