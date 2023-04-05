import { React, Component } from "react";
import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import ButtonGroup from '@mui/material/ButtonGroup';

class SelectTopic extends Component {

    constructor(props) {
        super(props);
        this.state = {
            topic: props.topic,
            topics: props.topics,
        };
        this.clickTopic = this.clickTopic.bind(this);
    }

    clickTopic = (event) => {
        event.preventDefault();
        this.setState((state, props) => {
            console.log(event.target.value)
            props.setTopic(event.target.value)
            return {
                ...state,
                topic: event.target.value,
            };
        }, () => {
            this.setState((state) => {
                return {
                    ...state,
                    topic: event.target.value,
                };
            });
        });
    };

    render() {
        let topicButtons = [];
        for (var i = 0; i < this.state.topics.length; i++) {
            let topic = this.state.topics[i]
            console.log(`topic button  : ${topic}`)
            topicButtons.push(
                <Button key={topic} value={topic} onClick={this.clickTopic}>{topic}</Button>
            )
        }
        return (
            <Box
                sx={{
                    display: 'flex',
                    '& > *': {
                        m: 1,
                    },
                }}
            >
                <ButtonGroup
                    orientation="vertical"
                    aria-label="vertical outlined button group"
                    variant="outlined"
                >
                    {topicButtons}
                </ButtonGroup>
            </Box>
        );
    }
};


export default (SelectTopic);
