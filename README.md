# spotisafe

Store any file inside Spotify playlist names. Because why not.

## How it works
Files are compressed, encoded as Base64, split into chunks, and stored
across multiple Spotify playlist names. Each playlist holds ~81 characters
of data. To retrieve a file, the chunks are reassembled in order, decoded,
and decompressed — producing a bit-for-bit identical copy of the original.

## Setup
1. Create a Spotify app at developer.spotify.com/dashboard
2. Add http://localhost:8888/callback as a Redirect URI
3. Install dependencies: pip install -r requirements.txt
4. Login: python main.py login

## Usage
python main.py upload <file> <name>     # store a file
python main.py download <name> <file>   # retrieve a file
python main.py list                     # list stored files
python main.py delete <name>            # delete a file

## Notes
- Works on Windows, Linux, and macOS
- Any file type supported
- Recommended for small files (<1MB)
- Requires a free or premium Spotify account
