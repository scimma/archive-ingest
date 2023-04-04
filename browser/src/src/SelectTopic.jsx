import { React, Component } from "react";
import Box from '@mui/material/Box';
// import Radio from '@mui/material/Radio';
// import RadioGroup from '@mui/material/RadioGroup';
// import FormControlLabel from '@mui/material/FormControlLabel';
// import FormControl from '@mui/material/FormControl';
// import FormLabel from '@mui/material/FormLabel';
// import FormHelperText from '@mui/material/FormHelperText';
import Button from '@mui/material/Button';
import ButtonGroup from '@mui/material/ButtonGroup';

class SelectTopic extends Component {

	constructor(props) {
		super(props);
		this.state = {
			topic: props.topic,
		};
		this.handleChange = this.handleChange.bind(this);
	}

	handleChange = (event) => {
		this.setState((state, props) => {
			props.setTopic(event.target.value);
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
		return (
			// <Box sx={{ display: 'flex' }}>
			// 	<FormControl sx={{ m: 3 }} component="fieldset" variant="standard">
			// 		<FormLabel component="legend">Select Hopskotch topic.</FormLabel>
			// 		<RadioGroup
			// 			aria-labelledby="demo-radio-buttons-group-label"
			// 			defaultValue="female"
			// 			name="radio-buttons-group"
			// 			value={this.state.topic}
			// 			onChange={this.handleChange}
			// 		>
			// 			<FormControlLabel value="topic1" control={<Radio />} label="topic1" />
			// 			<FormControlLabel value="topic2" control={<Radio />} label="topic2" />
			// 			<FormControlLabel value="topic3" control={<Radio />} label="topic3" />
			// 		</RadioGroup>
			// 		{/* <FormHelperText>Select a topic to browse.</FormHelperText> */}
			// 	</FormControl>
			// </Box>

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
					<Button value="topic1" onClick={this.handleChange}>topic1</Button>
					<Button value="topic2" onClick={this.handleChange}>topic2</Button>
					<Button value="topic3" onClick={this.handleChange}>topic3</Button>
				</ButtonGroup>
			</Box>
		);
	}
};


export default (SelectTopic);
