import get_data


def handler(event, context):
    get_data.run()
    return {'statusCode': 200, 'body': 'OK'}
