# Example Typescript code of merging multiple csv streams
Merge large csv files that have a common sorted column. 

## Install
Ensure you have NodeJs then run `npm i` in the source directory

## Using
Configure your code in src/index.ts then run via `npm start`, an output csv file will be created.

## Notes:
 - the output csv contains all of the headers from all of the input files by default
 - rows with the same id in multiple input csvs are merged in the order the inputs are defined, overriding any common properties
