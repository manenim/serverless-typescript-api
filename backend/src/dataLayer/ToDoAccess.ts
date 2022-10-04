import * as AWS from 'aws-sdk';
import * as AWSXRay from 'aws-xray-sdk'
import { DocumentClient } from 'aws-sdk/clients/dynamodb';
import { TodoItem } from '../models/TodoItem';
import { TodoUpdate } from '../models/TodoUpdate';

const XAWS = AWSXRay.captureAWS(AWS)

const s3 = new XAWS.S3({
    signatureVersion: 'v4'
})

export default class TodosAccess {

    constructor(
        private readonly docClient: DocumentClient = createDynamoDBClient(),
        private readonly todosTable = process.env.TODOS_TABLE,
        private readonly userIdIndex = process.env.USER_ID_INDEX,
        private readonly bucketName = process.env.ATTACHMENT_IMAGES_S3_BUCKET,
        private readonly urlExpiration = process.env.SIGNED_URL_EXPIRATION) {
    }

    async getAllTodosForUser(userId: string): Promise<TodoItem[]> {
        console.log(`Getting all todos for user ${userId}`)

        const result = await this.docClient.query({
            TableName: this.todosTable,
            IndexName: this.userIdIndex,
            KeyConditionExpression: 'userId = :userId',
            ExpressionAttributeValues: {
                ':userId': userId
            }
        }).promise()

        const items = result.Items;
        return items as TodoItem[];
    }

    async createTodo(todoItem: TodoItem): Promise<TodoItem> {
        await this.docClient.put({
            TableName: this.todosTable,
            Item: todoItem
        }).promise();

        return todoItem;
    }


    async updateTodo(todoId: string, updateTodoItem: TodoUpdate): Promise<void> {
        // form the update expression and the expression attribute values
        let updateExpression = '';
        const expressionAttributeValues = {};
        const expressionAttributeNames = {};

        if (updateTodoItem.name) {
            updateExpression = 'SET #name =:n';
            expressionAttributeValues[':n'] = updateTodoItem.name;
            expressionAttributeNames["#name"] = "name";
        }

        if (updateTodoItem.dueDate) {
            const dueDateTimestamp = Date.parse(updateTodoItem.dueDate);
            updateExpression = `${updateExpression}, #dueDate =:d`;
            expressionAttributeValues[':d'] = new Date(dueDateTimestamp).toISOString();
            expressionAttributeNames["#dueDate"] = "dueDate";
        }

        if (updateTodoItem.done) {
            updateExpression = `${updateExpression}, #done =:do`;
            expressionAttributeValues[':do'] = updateTodoItem.done;
            expressionAttributeNames["#done"] = "done";
        }

        if (updateExpression === '') {
            throw {
                status: 404,
                error: new Error("TODO does not exist")
            }
        }

        const params = {
            TableName: this.todosTable,
            Key: {
                todoId: todoId
            },
            UpdateExpression: updateExpression,
            ExpressionAttributeNames: expressionAttributeNames,
            ExpressionAttributeValues: expressionAttributeValues,
            ReturnValues: 'UPDATED_NEW'
        };

        await this.docClient.update(params).promise();
    }

    async deleteTodo(todoId: string): Promise<void> {
        const params = {
            TableName: this.todosTable,
            Key: {
                todoId: todoId
            },
            ReturnValues: 'ALL_OLD'
        };
        await this.docClient.delete(params).promise();
    }

    async generateUploadUrl(todoId: string, imageId: string): Promise<string> {
        const params = {
            TableName: this.todosTable,
            Key: {
                todoId: todoId
            },
            UpdateExpression: 'SET #attachmentUrl =:url',
            ExpressionAttributeNames: { "#attachmentUrl": "attachmentUrl" },
            ExpressionAttributeValues: { ":url": `https://${this.bucketName}.s3.amazonaws.com/${imageId}` },
            ReturnValues: 'UPDATED_NEW'
        };

        // update the db with the get url for the image before sending out the 
        // upload url
        await this.docClient.update(params).promise();
        const url = this.getUploadUrl(imageId);
        return url;
    }

    async todoExists(todoId: string) {
        const result = await this.docClient
            .get({
                TableName: this.todosTable,
                Key: {
                    todoId,
                }
            })
            .promise()
        console.log('Get Todo: ', result)
        return !!result.Item

    }

    async getUploadUrl(imageId: string) {
        return s3.getSignedUrl('putObject', {
            Bucket: this.bucketName,
            Key: imageId,
            Expires: this.urlExpiration
        })
    }

}

function createDynamoDBClient() {
    if (process.env.IS_OFFLINE) {
        console.log('Creating a local DynamoDB instance')
        return new AWS.DynamoDB.DocumentClient({
            region: 'localhost',
            endpoint: 'http://localhost:8000'
        })
    }

    return new XAWS.DynamoDB.DocumentClient()
}
