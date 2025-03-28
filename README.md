# Marathon

## About

`marathon` is workflow execution engine, allowing for performant execution of various inter-connected tasks while still giving the user control over how the state is managed. This tool is designed to be used with Scaffold as the base execution engine for Data Flows.

Currently only go is supported as a language for steps, however we plan to add support for Python, Bash, and other languages in the future. 

Additionally, resources have not been fully implemented yet, however that will be coming in a future update.

## Usage

See [the test program](./test/main.go) for a complete example of how to use `marathon`

## Testing

```sh
./test/test.sh
```
