import { Component } from "react";
import {
	Container,
	Grid,
	Typography,
} from '@mui/material';
import SelectTopic from './SelectTopic.jsx'
import { createTheme, ThemeProvider } from '@mui/material/styles';
import Divider from '@mui/material/Divider';
import config from "./app.config";


const theme = createTheme({
	palette: {
		primary: {
			// main: "#18381b"
			main: "#5daefd"
		},
		secondary: {
			main: "#e8a114"
		},
		third: {
			main: "#333333"
		},
		fourth: {
			main: "#eeeeee"
		}
	},
	typography: {
		h1: {
			fontFamily: "'Roboto Condensed',sans-serif"
		},
		h2: {
			fontFamily: "'Roboto', 'Helvetica', 'Arial', sans-serif"
		},
		h3: {
			fontFamily: "'Roboto', 'Helvetica', 'Arial', sans-serif"
		},
		h4: {
			fontFamily: "'Roboto', 'Helvetica', 'Arial', sans-serif"
		},
		h5: {
			fontFamily: "'Roboto', 'Helvetica', 'Arial', sans-serif",
			fontSize: { xs: '1rem', sm: '2rem' },
		},
		h6: {
			fontFamily: "'Roboto', 'Helvetica', 'Arial', sans-serif"
		},
		body1: {
			fontFamily: "'Work Sans',sans-serif",
			fontSize: "1rem"
		},
		body2: {
			fontFamily: "'Work Sans',sans-serif",
			fontSize: "2rem"
		},
		bold: {
			fontFamily: "'Work Sans',sans-serif",
			fontWeight: 800,
		}
	},
});


class App extends Component {

	constructor(props) {
		super(props);
		this.state = {
			topic: null
		}
		this.setTopic = this.setTopic.bind(this);
		this.testApi = this.testApi.bind(this);
		this.handleResponse = this.handleResponse.bind(this);
	}

	setTopic(selectedTopic) {
		this.setState({
			topic: selectedTopic
		}, () => {
			console.log(`topic: ${selectedTopic}`)
			this.testApi()
		})
	}

	async testApi() {
		const endpoint = `${config.protocol}://${config.hostname}:${config.port}${config.baseApiUrl}${config.endpointTest}`;
		// const endpoint = `${config.baseApiUrl}${config.endpointTest}`;
		const payload = {
			param1: this.state.topic,
			param2: "param1 is the selected topic",
		};
		let apiTest = await fetch(endpoint, {
			method: "POST",
			headers: {
				"Content-Type": "application/json",
			},
			body: JSON.stringify(payload)
		});
		this.handleResponse(apiTest)
	}

	async handleResponse(response) {
		if (response.status >= 200 && response.status <= 299) {
			let returnedData = await response.json();
			console.log(JSON.stringify(returnedData, null, 2));
		} else {
			console.log(JSON.stringify(response.statusText, null, 2));
		}
	}

	render() {
		let selectedTopic = <></>;
		if (this.state.topic !== null) {
			selectedTopic = (
				<Typography>
					Downloading messages in topic <Typography variant="bold">{this.state.topic}</Typography>...
				</Typography>
			)
		}
		return (
			<ThemeProvider theme={theme}>
				<Container>
					<Grid container spacing={4}>
						<Grid item md={12}>
							<Typography variant="h2">
								SCiMMA Archive Browser
							</Typography>
						</Grid>
					</Grid>
					<Divider />
					<Grid container spacing={4} marginTop={0} >
						<Grid item sm={12} md={3}>
							<Typography>
								Select a topic to browse the SCiMMA Hopskotch message archive.
							</Typography>
						</Grid>
						<Grid item sm={12} md={9}>
							<SelectTopic setTopic={this.setTopic} topic={this.state.topic} />
						</Grid>
					</Grid>
					<Grid container spacing={4} marginTop={2} >
						<Grid item md={12}>
							{selectedTopic}
						</Grid>
					</Grid>
				</Container>
			</ThemeProvider>
		);
	}
}

export default App;
