import { Component } from "react";
import {
  Container,
  AppBar,
  Link,
  Box,
  Toolbar,
  Typography,
  IconButton,
  Grid,
  Button,
  ButtonGroup,
  Dialog,
  DialogActions,
  DialogContent,
  DialogContentText,
  DialogTitle,
  InputLabel,
  MenuItem,
  FormControl,
  Select,
} from '@mui/material';
import MenuIcon from '@mui/icons-material/Menu';
import { createTheme, ThemeProvider } from '@mui/material/styles';
import { DataGrid, GridToolbar } from '@mui/x-data-grid';
import { withStyles } from '@mui/styles';
import CssBaseline from '@mui/material/CssBaseline';
import config from "./app.config";
// import SelectTopic from './SelectTopic.jsx'
import TextField from '@mui/material/TextField';
import { AdapterDayjs } from '@mui/x-date-pickers/AdapterDayjs';
import { LocalizationProvider } from '@mui/x-date-pickers/LocalizationProvider';
import { DatePicker } from '@mui/x-date-pickers/DatePicker';


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
      fontSize: "1rem"
    }
  },
});

const styles = {
  menuCustomWidth: {
    "& li": {
      width: "200px"
    }
  },
  appBar: {
    width: "100%",
    transition: theme.transitions.create(["margin", "width"], {
      easing: theme.transitions.easing.sharp,
      duration: theme.transitions.duration.leavingScreen,
    }),
  },
  container: {
    "&": {
      backgroundColor: "#ffffff",
      marginTop: theme.spacing(3),
      marginBottom: theme.spacing(1),
      display: "flex",
      flexDirection: "column",
      alignItems: "center",
    },
  },
  messageTableContainer: {
    marginTop: theme.spacing(0),
  },
  avatarImg: {
    width: "32px",
    height: "32px",
    borderRadius: "50%",
    // border:"solid 2px #FFFFFF"
  },
  toolBar: {
    minHeight: "48px",
    display: "flex",
    justifyContent: "flex-start",
  },
  smallButton: {
    padding: "6px"
  },
  denseStyle: {
    minHeight: "10px",
    lineHeight: "30px",
    fontSize: "12px",
  },
  status: {
    padding: "6px 16px"
  },
  fontLight: {
    fontSize: "12px",
    color: "#333333",
    fontWeight: 100,
    fontFamily: theme.typography.body1.fontFamily,
  },
  fontBold: {
    fontSize: "12px",
    color: "#000000",
    fontWeight: 600,
    fontFamily: theme.typography.body1.fontFamily,
  },
  toolBarItem: {
    margin: "auto 20px",
    cursor: "pointer"
  },
  customProgressBar: {
    borderRadius: 5,
    height: 5,
  },
  bannerText: {
    "&:hover": {
      cursor: "pointer"
    },
  },
  selectedButton: {
    backgroundColor: "red",
  }
};

class App extends Component {

  constructor(props) {
    super(props);
		let startDate = new Date();
    startDate.setDate(startDate.getDate() - 7);
		let endDate = new Date();
    endDate.setDate(endDate.getDate() + 1);
    this.state = {
      topic: '',
      topics: [],
      messages: [],
      sortModel: [
        {
          field: 'timestamp',
          sort: 'desc',
        }],
      details: {
        open: false,
        id: null,
        bodyText: "",
      },
      welcome: {
        open: true,
      },
      startDate: startDate,
      endDate: endDate,
    }
    this.setStartDate = this.setStartDate.bind(this);
    this.setEndDate = this.setEndDate.bind(this);
    this.setTopic = this.setTopic.bind(this);
    this.getTopics = this.getTopics.bind(this);
    this.listTopic = this.listTopic.bind(this);
    this.setSortModel = this.setSortModel.bind(this);
    this.clickTopic = this.clickTopic.bind(this);
    this.closeDetails = this.closeDetails.bind(this);
    this.convertDateString = this.convertDateString.bind(this);
  }


	setStartDate(date) {
		this.setState({
			startDate: date
		})
	}

	setEndDate(date) {
		this.setState({
			endDate: date
		})
	}

	convertDateString(date) {
		if (date == null) {
			return date
		} else {
			return date.toISOString().split('T')[0]
		}
	}

  async listInDateRange() {
    const endpoint = `${config.protocol}://${config.hostname}:${config.port}${config.baseApiUrl}/topic/range`;
    const payload = {
      start_date: this.convertDateString(this.state.startDate),
      end_date: this.convertDateString(this.state.endDate),
    };
    let response = await fetch(endpoint, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(payload)
    });
    if (response.status >= 200 && response.status <= 299) {
      let data = await response.json();
      // console.log(JSON.stringify(data, null, 2));
      let messages = data.messages.map((message, index, array) => {
        return {
          id: message['uuid'],
          timestamp: new Date(message['timestamp']),
          key: message['key'],
          topic: message['topic'],
        }
      });
      this.setState({
        messages: messages,
      })
    } else {
      console.log(JSON.stringify(response.statusText, null, 2));
    }
  }

  async listTopic() {
    const endpoint = `${config.protocol}://${config.hostname}:${config.port}${config.baseApiUrl}/topic/${this.state.topic}`;
    let response = await fetch(endpoint, {
      method: "GET",
      // body: JSON.stringify(payload)
    });
    // this.handleResponselistTopic(response)
    if (response.status >= 200 && response.status <= 299) {
      let data = await response.json();
      // console.log(JSON.stringify(data, null, 2));
      let messages = data.messages.map((message, index, array) => {
        return {
          id: message['uuid'],
          timestamp: new Date(message['timestamp']),
          key: message['key'],
          topic: message['topic'],
        }
      });
      this.setState({
        messages: messages,
      })
    } else {
      console.log(JSON.stringify(response.statusText, null, 2));
    }
  }

  async getTopics() {
    const endpoint = `${config.protocol}://${config.hostname}:${config.port}${config.baseApiUrl}/topics`;
    let response = await fetch(endpoint, {
      method: "GET",
    });
    if (response.status >= 200 && response.status <= 299) {
      let data = await response.json();
      // console.log(JSON.stringify(data, null, 2));
      this.setState({
        topics: data['topics'],
      })
    } else {
      console.log(JSON.stringify(response.statusText, null, 2));
    }
  }

  setSortModel(model) {
    this.setState({
      sortModel: model,
    })
  };

  columns = [
    {
      field: 'topic',
      headerName: 'Topic',
      description: 'Hopskotch topic name',
      sortable: true,
      flex: 1,
      minWidth: 50,
    },
    {
      field: 'timestamp',
      headerName: 'Time',
      description: 'Time stamp (UTC)',
      type: 'dateTime',
      sortable: true,
      flex: 1,
      minWidth: 50,
    },
    {
      field: 'id',
      headerName: 'ID',
      description: 'Unique message ID',
      sortable: false,
      flex: 1,
      minWidth: 50
    },
    {
      field: 'key',
      headerName: 'Key',
      description: 'Message key',
      sortable: true,
      flex: 1,
      minWidth: 50
    },
  ]

  componentDidMount() {
    this.getTopics()
  }

  clickDateSearch = (event) => {
    event.preventDefault();
    this.setState({
      topic: ''
    }, () => {
      this.listInDateRange()
    })
  }

  clickTopic = (event) => {
    event.preventDefault();
    this.setTopic(event.target.value)
  }

  setTopic(selectedTopic) {
    this.setState({
      topic: selectedTopic
    }, () => {
      // console.log(`topic: ${selectedTopic}`)
      this.listTopic()
    })
  }

	async fetchDetails(id) {
    const endpoint = `${config.protocol}://${config.hostname}:${config.port}${config.baseApiUrl}/message/${id}`;
    let response = await fetch(endpoint, {
      method: "GET",
    });
    if (response.status >= 200 && response.status <= 299) {
      let data = await response.json();
      // console.log( JSON.stringify(data, null, 2));
      let metadataJson = JSON.stringify(data.details.metadata, null, 2)
      // console.log(metadataJson);
      let messageJson = JSON.stringify(data.details.message, null, 2)
      // console.log(messageJson);
      this.setState({
        details: {
          // ...details,
          id: id,
          bodyText: `metadata:\n${metadataJson}\n\nmessage:\n${messageJson}`,
          open: true,
        }
      })
    } else {
      console.log(JSON.stringify(response.statusText, null, 2));
    }
  }

  closeDetails() {
    this.setState({
      details: {
        id: null,
        bodyText: "",
        open: false,
      },
      welcome: {
        open: false,
      }
    })
  }

  render() {
    const { classes } = this.props;
    const drawerWidth = 240;
    let selectedTopic = <></>;
    // if (this.state.topic !== null) {
      //   selectedTopic = (
        //     <Typography>
        //       Downloading messages in topic <Typography variant="bold">{this.state.topic}</Typography>...
        //     </Typography>
        //   )
        // }
        let topicButtons = [];
        let topicSelectItems = [];
    for (var i = 0; i < this.state.topics.length; i++) {
      let topic = this.state.topics[i]
      topicButtons.push(
        <Button key={topic} value={topic} onClick={this.clickTopic}>{topic}</Button>
        )
        topicSelectItems.push(
          <MenuItem key={topic} value={topic}><code>{topic}</code></MenuItem>
          )
        }
    let dataGrid = <></>;
    if (this.state.messages.length > 0) {
      dataGrid = (
        <DataGrid
          sx={{
            height: '50vh',
            width: '100%',
          }}
          rows={this.state.messages}
          columns={this.columns}
          // autoHeight
          autoPageSize={false}
          pageSizeOptions={[20, 50, 100]}
          onRowClick={(params, event, details) => {this.fetchDetails(params.id)}}
          onSortModelChange={(model) => this.setSortModel(model)}
          sortModel={this.state.sortModel}
          slots={{ toolbar: GridToolbar }}
          disableRowSelectionOnClick
        />
      )
    }

    return (
      <ThemeProvider theme={theme}>
        <CssBaseline />
        <Box sx={{ display: 'flex' }}>

          <AppBar position="fixed"
            sx={{
              zIndex: (theme) => theme.zIndex.drawer + 1,
              // width: { sm: `calc(100% - ${drawerWidth}px)` },
              // ml: { sm: `${drawerWidth}px` },
            }}
            className={classes.appBar}>
            <Toolbar className={classes.toolBar}>
              <IconButton
                color="inherit"
                aria-label="open drawer"
                edge="start"
                // onClick={this.handleDrawerToggle}
                sx={{ mr: 2, display: { sm: 'none' } }}
              >
                <MenuIcon />
              </IconButton>
              <Link className={classes.bannerText} style={{ color: "#ffffff", textDecoration: "none" }}
                sx={{
                  fontSize: { xs: '1rem', sm: '1.5rem' },
                }}
              // onClick={(event) => {
              // 	// event.stopPropagation();
              // 	browserHistory.push(`${config.baseUrl}`);
              // }}
              >
                <Typography variant="h5">
                  SCiMMA // Archive Browser
                </Typography>
              </Link>
              <Typography variant="body1" style={{ flex: 1 }} />
            </Toolbar>
          </AppBar>
          <Box
            component="main"
            sx={{ flexGrow: 1, p: 3, width: { xs: '100%', sm: `calc(100% - ${drawerWidth}px)` } }}
          >

            <Container className={classes.container}>
              <Grid container spacing={4} marginTop={0} >
                <Grid item xs={12} md={4}>
                  <Grid container spacing={4} marginTop={0} >
                    <Grid item xs={12} md={12}>
                      <Typography>
                        Select a SCiMMA Hopskotch topic to list all archived messages in that topic.
                      </Typography>
                    </Grid>
                    <Grid item xs={12} md={12}>
                      <Box
                        sx={{
                          display: 'flex',
                          '& > *': {
                            m: 1,
                          },
                        }}
                      >
                        {/* <ButtonGroup
                          orientation="horizontal"
                          aria-label="horizontal outlined button group"
                          variant="outlined"
                        >
                          {topicButtons}
                        </ButtonGroup> */}

                        <FormControl fullWidth>
                          <InputLabel id="select-topic">Topic</InputLabel>
                          <Select
                            labelId="select-topic"
                            id="select-topic"
                            value={this.state.topic}
                            label="Topic"
                            onChange={this.clickTopic}
                            // renderValue={(selected) => {
                            //   if (selected.length === 0) {
                            //     return <em>Select a topic</em>;
                            //   }
                            //   return selected.join(', ');
                            // }}
                          >
                            {topicSelectItems}
                          </Select>
                        </FormControl>
                        </Box>
                    </Grid>
                  </Grid>
                </Grid>
                <Grid item xs={12} md={8}>
                  <Grid container spacing={4} marginTop={0} >
                    <Grid item xs={12} md={6}>
                          <Typography> Search for messages across all archived topics within the selected date range.</Typography>
                    </Grid>
                    <Grid item xs={12} md={6}>
                        <Box
                        sx={{
                          display: 'flex',
                          '& > *': {
                            m: 1,
                          },
                        }}
                        >
                        <ButtonGroup
                          orientation="horizontal"
                          aria-label="horizontal outlined button group"
                          variant="outlined"
                          >
                          <Button key="dateSearch" value="dateSearch" onClick={this.clickDateSearch}>Search</Button>
                        </ButtonGroup>
                      </Box>
                    </Grid>
                    <Grid item xs={12} md={4}>
                          <LocalizationProvider dateAdapter={AdapterDayjs}>
                            <DatePicker
                              label="Start Date"
                              value={this.state.startDate }
                              onChange={this.setStartDate}
                              renderInput={(params) => <TextField {...params} sx={{ width: '80%', minWidth: '12rem'}}/>}
                              />
                          </LocalizationProvider>
                        </Grid>
                    <Grid item xs={12} md={4}>
                          <LocalizationProvider dateAdapter={AdapterDayjs}>
                            <DatePicker
                              label="End Date"
                              value={this.state.endDate }
                              onChange={this.setEndDate}
                              renderInput={(params) => <TextField {...params} sx={{ width: '80%', minWidth: '12rem'}}/>}
                              />
                          </LocalizationProvider>
                        </Grid>
                  </Grid>
                </Grid>
              </Grid>
              <Grid container spacing={4} marginTop={2} >
                <Grid item md={12}>
                  {selectedTopic}
                </Grid>
              </Grid>
            </Container>
            <Container className={classes.messageTableContainer}>

              {dataGrid}

              <Dialog
                open={this.state.details.open}
                onClose={this.closeDetails}
                scroll={'paper'}
                aria-labelledby="scroll-dialog-title"
                aria-describedby="scroll-dialog-description"
                fullWidth={true}
                maxWidth="lg"
              >
                <DialogTitle id="scroll-dialog-title">Message ID: <code>{this.state.details.id}</code></DialogTitle>
                <DialogContent dividers={true}>
                  <DialogContentText
                    id="scroll-dialog-description"
                    // ref={descriptionElementRef}
                    tabIndex={-1}
                  >
                    <Typography sx={{whiteSpace: "pre-wrap"}}>{this.state.details.bodyText}</Typography>
                  </DialogContentText>
                </DialogContent>
                <DialogActions>
                  <Button onClick={this.closeDetails}>Close</Button>
                </DialogActions>
              </Dialog>


              <Dialog
                open={this.state.welcome.open}
                onClose={this.closeDetails}
                scroll={'paper'}
                aria-labelledby="scroll-dialog-title"
                aria-describedby="scroll-dialog-description"
                fullWidth={true}
                maxWidth="md"
              >
                <DialogTitle id="scroll-dialog-title">Welcome to the Hopskotch Archive Browser!</DialogTitle>
                <DialogContent dividers={true}>
                  <DialogContentText
                    id="scroll-dialog-description"
                    // ref={descriptionElementRef}
                    tabIndex={-1}
                  >
                    <Box marginBottom={1}>

                    <Typography>
                    <Link href="https://hop.scimma.org" target="_blank"><b>Hopskotch</b></Link> is the protocol powering the 
                    event streaming hub for multi-messenger astronomy operated by the <Link href="https://scimma.org" target="_blank">SCiMMA</Link> project.
                    The Hopskotch Archive System permanently stores public messages from
                    the ephemeral data streams, or "topics", and 
                    provides a RESTful web API for the scientific community to utilize the data.
                    </Typography>
                    </Box>

                    <Box marginBottom={1}>
                    <Typography>
                    This Archive Browser is a web app designed as an <b>interactive demonstration</b> of a small fraction of the functionality we envision for a fully developed
                      archive system. It shows how researchers can easily list messages in topics of interest and download the message data. 
                      Queries of arbitrary complexity against the archive's document database are possible; here, we sample this capability by listing  
                      messages across all archived topics within a specified date range.
                    </Typography>
                    </Box>
                    <Box>

                    <Typography>
                      For more details about the system architecture, as well as the open source code, please
                      see <Link href="https://github.com/scimma/archive-ingest" target="_blank">our git repo here</Link>.
                      
                    </Typography>
                    </Box>
                  </DialogContentText>
                </DialogContent>
                <DialogActions>
                  <Button onClick={this.closeDetails}>Close</Button>
                </DialogActions>
              </Dialog>


            </Container>
          </Box>
        </Box>
      </ThemeProvider>
    );
  }
}

export default withStyles(styles)(App);
